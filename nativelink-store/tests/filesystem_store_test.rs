// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime};

use async_lock::RwLock;
use bytes::Bytes;
use filetime::{set_file_atime, FileTime};
use futures::executor::block_on;
use futures::task::Poll;
use futures::{poll, Future, FutureExt};
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{
    digest_from_filename, EncodedFilePath, FileEntry, FileEntryImpl, FilesystemStore,
};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::{fs, DigestInfo};
use nativelink_util::evicting_map::LenEntry;
use nativelink_util::origin_context::ContextAwareFuture;
use nativelink_util::store_trait::{Store, StoreLike, UploadSizeInfo};
use nativelink_util::{background_spawn, spawn};
use parking_lot::Mutex;
use pretty_assertions::assert_eq;
use rand::{thread_rng, Rng};
use serial_test::serial;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Barrier;
use tokio::time::sleep;
use tokio_stream::wrappers::ReadDirStream;
use tokio_stream::StreamExt;
use tracing::Instrument;

trait FileEntryHooks {
    fn on_unref<Fe: FileEntry>(_entry: &Fe) {}
    fn on_drop<Fe: FileEntry>(_entry: &Fe) {}
}

struct TestFileEntry<Hooks: FileEntryHooks + 'static + Sync + Send> {
    inner: Option<FileEntryImpl>,
    _phantom: PhantomData<Hooks>,
}

impl<Hooks: FileEntryHooks + 'static + Sync + Send> Debug for TestFileEntry<Hooks> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("TestFileEntry")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<Hooks: FileEntryHooks + 'static + Sync + Send> FileEntry for TestFileEntry<Hooks> {
    fn create(data_size: u64, block_size: u64, encoded_file_path: RwLock<EncodedFilePath>) -> Self {
        Self {
            inner: Some(FileEntryImpl::create(
                data_size,
                block_size,
                encoded_file_path,
            )),
            _phantom: PhantomData,
        }
    }

    async fn make_and_open_file(
        block_size: u64,
        encoded_file_path: EncodedFilePath,
    ) -> Result<(Self, fs::ResumeableFileSlot, OsString), Error> {
        let (inner, file_slot, path) =
            FileEntryImpl::make_and_open_file(block_size, encoded_file_path).await?;
        Ok((
            Self {
                inner: Some(inner),
                _phantom: PhantomData,
            },
            file_slot,
            path,
        ))
    }

    fn data_size_mut(&mut self) -> &mut u64 {
        self.inner.as_mut().unwrap().data_size_mut()
    }

    fn size_on_disk(&self) -> u64 {
        self.inner.as_ref().unwrap().size_on_disk()
    }

    fn get_encoded_file_path(&self) -> &RwLock<EncodedFilePath> {
        self.inner.as_ref().unwrap().get_encoded_file_path()
    }

    async fn read_file_part(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<fs::ResumeableFileSlot, Error> {
        self.inner
            .as_ref()
            .unwrap()
            .read_file_part(offset, length)
            .await
    }

    async fn get_file_path_locked<
        T,
        Fut: Future<Output = Result<T, Error>> + Send,
        F: FnOnce(OsString) -> Fut + Send,
    >(
        &self,
        handler: F,
    ) -> Result<T, Error> {
        self.inner
            .as_ref()
            .unwrap()
            .get_file_path_locked(handler)
            .await
    }
}

impl<Hooks: FileEntryHooks + 'static + Sync + Send> LenEntry for TestFileEntry<Hooks> {
    fn len(&self) -> u64 {
        self.inner.as_ref().unwrap().len()
    }

    fn is_empty(&self) -> bool {
        self.inner.as_ref().unwrap().is_empty()
    }

    async fn unref(&self) {
        Hooks::on_unref(self);
        self.inner.as_ref().unwrap().unref().await;
    }
}

impl<Hooks: FileEntryHooks + 'static + Sync + Send> Drop for TestFileEntry<Hooks> {
    fn drop(&mut self) {
        let mut inner = self.inner.take().unwrap();
        let shared_context = inner.get_shared_context_for_test();
        // We do this complicated bit here because tokio does not give a way to run a
        // command that will wait for all tasks and sub spawns to complete.
        // Sadly we need to rely on `active_drop_spawns` to hit zero to ensure that
        // all tasks have completed.
        let fut = ContextAwareFuture::new_from_active(
            async move {
                // Drop the FileEntryImpl in a controlled setting then wait for the
                // `active_drop_spawns` to hit zero.
                drop(inner);
                while shared_context.active_drop_spawns.load(Ordering::Acquire) > 0 {
                    tokio::task::yield_now().await;
                }
            }
            .instrument(tracing::error_span!("test_file_entry_drop")),
        );
        #[allow(clippy::disallowed_methods)]
        let thread_handle = {
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(fut);
            })
        };
        thread_handle.join().unwrap();
        // At this point we can guarantee our file drop spawn has completed.
        Hooks::on_drop(self);
    }
}

/// Get temporary path from either `TEST_TMPDIR` or best effort temp directory if
/// not set.
fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR").unwrap_or(env::temp_dir().to_str().unwrap().to_string()),
        thread_rng().gen::<u64>(),
        data
    )
}

async fn read_file_contents(file_name: &OsStr) -> Result<Vec<u8>, Error> {
    let mut file = fs::open_file(file_name, u64::MAX)
        .await
        .err_tip(|| format!("Failed to open file: {file_name:?}"))?;
    let mut data = vec![];
    file.as_reader()
        .await?
        .read_to_end(&mut data)
        .await
        .err_tip(|| "Error reading file to end")?;
    Ok(data)
}

async fn write_file(file_name: &OsStr, data: &[u8]) -> Result<(), Error> {
    let mut file = fs::create_file(file_name)
        .await
        .err_tip(|| format!("Failed to create file: {file_name:?}"))?;
    file.as_writer().await?.write_all(data).await?;
    file.as_writer()
        .await?
        .as_mut()
        .sync_all()
        .await
        .err_tip(|| "Could not sync file")
}

async fn wait_for_no_open_files() -> Result<(), Error> {
    let mut counter = 0;
    while fs::get_open_files_for_test() != 0 {
        sleep(Duration::from_millis(1)).await;
        counter += 1;
        if counter > 1000 {
            return Err(make_err!(
                Code::Internal,
                "Timed out waiting all files to close"
            ));
        }
    }
    Ok(())
}

const HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
const VALUE1: &str = "0123456789";
const VALUE2: &str = "9876543210";

#[serial]
#[nativelink_test]
async fn valid_results_after_shutdown_test() -> Result<(), Error> {
    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");
    {
        let store = Store::new(
            FilesystemStore::<FileEntryImpl>::new(&nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                eviction_policy: None,
                block_size: 1,
                ..Default::default()
            })
            .await?,
        );

        // Insert dummy value into store.
        store.update_oneshot(digest, VALUE1.into()).await?;

        assert_eq!(
            store.has(digest).await,
            Ok(Some(VALUE1.len() as u64)),
            "Expected filesystem store to have hash: {}",
            HASH1
        );
    }
    {
        // With a new store ensure content is still readable (ie: restores from shutdown).
        let store = Box::pin(
            FilesystemStore::<FileEntryImpl>::new(&nativelink_config::stores::FilesystemStore {
                content_path,
                temp_path,
                eviction_policy: None,
                ..Default::default()
            })
            .await?,
        );

        let content = store.get_part_unchunked(digest, 0, None).await?;
        assert_eq!(content, VALUE1.as_bytes());
    }

    Ok(())
}

#[serial]
#[nativelink_test]
async fn temp_files_get_deleted_on_replace_test() -> Result<(), Error> {
    static DELETES_FINISHED: AtomicU32 = AtomicU32::new(0);
    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_drop<Fe: FileEntry>(_file_entry: &Fe) {
            DELETES_FINISHED.fetch_add(1, Ordering::Relaxed);
        }
    }

    let digest1 = DigestInfo::try_new(HASH1, VALUE1.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Box::pin(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
                    max_count: 3,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await?,
    );

    store.update_oneshot(digest1, VALUE1.into()).await?;

    let expected_file_name = OsString::from(format!("{content_path}/{digest1}"));
    {
        // Check to ensure our file exists where it should and content matches.
        let data = read_file_contents(&expected_file_name).await?;
        assert_eq!(
            &data[..],
            VALUE1.as_bytes(),
            "Expected file content to match"
        );
    }

    // Replace content.
    store.update_oneshot(digest1, VALUE2.into()).await?;

    {
        // Check to ensure our file now has new content.
        let data = read_file_contents(&expected_file_name).await?;
        assert_eq!(
            &data[..],
            VALUE2.as_bytes(),
            "Expected file content to match"
        );
    }

    loop {
        if DELETES_FINISHED.load(Ordering::Relaxed) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }

    let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
        .await
        .err_tip(|| "Failed opening temp directory")?
        .into_inner();
    let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);

    if let Some(temp_dir_entry) = read_dir_stream.next().await {
        let path = temp_dir_entry?.path();
        panic!("No files should exist in temp directory, found: {path:?}");
    }

    Ok(())
}

// This test ensures that if a file is overridden and an open stream to the file already
// exists, the open stream will continue to work properly and when the stream is done the
// temporary file (of the object that was deleted) is cleaned up.
#[serial]
#[nativelink_test]
async fn file_continues_to_stream_on_content_replace_test() -> Result<(), Error> {
    static DELETES_FINISHED: AtomicU32 = AtomicU32::new(0);
    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_drop<Fe: FileEntry>(_file_entry: &Fe) {
            DELETES_FINISHED.fetch_add(1, Ordering::Relaxed);
        }
    }

    let digest1 = DigestInfo::try_new(HASH1, VALUE1.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
                    max_count: 3,
                    ..Default::default()
                }),
                block_size: 1,
                read_buffer_size: 1,
            },
        )
        .await?,
    );

    // Insert data into store.
    store.update_oneshot(digest1, VALUE1.into()).await?;

    let (writer, mut reader) = make_buf_channel_pair();
    let store_clone = store.clone();
    let digest1_clone = digest1;
    background_spawn!(
        "file_continues_to_stream_on_content_replace_test_store_get",
        async move { store_clone.get(digest1_clone, writer).await },
    );

    {
        // Check to ensure our first byte has been received. The future should be stalled here.
        let first_byte = reader
            .consume(Some(1))
            .await
            .err_tip(|| "Error reading first byte")?;
        assert_eq!(
            first_byte[0],
            VALUE1.as_bytes()[0],
            "Expected first byte to match"
        );
    }

    // Replace content.
    store.update_oneshot(digest1, VALUE2.into()).await?;

    // Ensure we let any background tasks finish.
    tokio::task::yield_now().await;

    {
        // Now ensure we only have 1 file in our temp path.
        let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
            .await
            .err_tip(|| "Failed opening temp directory")?
            .into_inner();
        let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);
        let mut num_files = 0;
        while let Some(temp_dir_entry) = read_dir_stream.next().await {
            num_files += 1;
            let path = temp_dir_entry?.path();
            let data = read_file_contents(path.as_os_str()).await?;
            assert_eq!(
                &data[..],
                VALUE1.as_bytes(),
                "Expected file content to match"
            );
        }
        assert_eq!(
            num_files, 1,
            "There should only be one file in the temp directory"
        );
    }

    let remaining_file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading remaining bytes")?;

    assert_eq!(
        &remaining_file_data,
        VALUE1[1..].as_bytes(),
        "Expected file content to match"
    );

    loop {
        if DELETES_FINISHED.load(Ordering::Relaxed) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }

    {
        // Now ensure our temp file was cleaned up.
        let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
            .await
            .err_tip(|| "Failed opening temp directory")?
            .into_inner();
        let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);
        if let Some(temp_dir_entry) = read_dir_stream.next().await {
            let path = temp_dir_entry?.path();
            panic!("No files should exist in temp directory, found: {path:?}");
        }
    }

    Ok(())
}

// Eviction has a different code path than a file replacement, so we check that if a
// file is evicted and has an open stream on it, it will stay alive and eventually
// get deleted.
#[serial]
#[nativelink_test]
async fn file_gets_cleans_up_on_cache_eviction() -> Result<(), Error> {
    static DELETES_FINISHED: AtomicU32 = AtomicU32::new(0);
    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_drop<Fe: FileEntry>(_file_entry: &Fe) {
            DELETES_FINISHED.fetch_add(1, Ordering::Relaxed);
        }
    }

    let digest1 = DigestInfo::try_new(HASH1, VALUE1.len())?;
    let digest2 = DigestInfo::try_new(HASH2, VALUE2.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
                    max_count: 1,
                    ..Default::default()
                }),
                block_size: 1,
                read_buffer_size: 1,
            },
        )
        .await?,
    );

    // Insert data into store.
    store.update_oneshot(digest1, VALUE1.into()).await?;

    let mut reader = {
        let (writer, reader) = make_buf_channel_pair();
        let store_clone = store.clone();
        background_spawn!(
            "file_gets_cleans_up_on_cache_eviction_store_get",
            async move { store_clone.get(digest1, writer).await },
        );
        reader
    };
    // Ensure we have received 1 byte in our buffer. This will ensure we have a reference to
    // our file open.
    assert!(reader.peek().await.is_ok(), "Could not peek into reader");

    // Insert new content. This will evict the old item.
    store.update_oneshot(digest2, VALUE2.into()).await?;

    // Ensure we let any background tasks finish.
    tokio::task::yield_now().await;

    {
        // Now ensure we only have 1 file in our temp path.
        let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
            .await
            .err_tip(|| "Failed opening temp directory")?
            .into_inner();
        let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);
        let mut num_files = 0;
        while let Some(temp_dir_entry) = read_dir_stream.next().await {
            num_files += 1;
            let path = temp_dir_entry?.path();
            let data = read_file_contents(path.as_os_str()).await?;
            assert_eq!(
                &data[..],
                VALUE1.as_bytes(),
                "Expected file content to match"
            );
        }
        assert_eq!(
            num_files, 1,
            "There should only be one file in the temp directory"
        );
    }

    let reader_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading remaining bytes")?;

    assert_eq!(&reader_data, VALUE1, "Expected file content to match");

    loop {
        if DELETES_FINISHED.load(Ordering::Relaxed) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }

    {
        // Now ensure our temp file was cleaned up.
        let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
            .await
            .err_tip(|| "Failed opening temp directory")?
            .into_inner();
        let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);
        if let Some(temp_dir_entry) = read_dir_stream.next().await {
            let path = temp_dir_entry?.path();
            panic!("No files should exist in temp directory, found: {path:?}");
        }
    }

    Ok(())
}

#[serial]
#[nativelink_test]
async fn oldest_entry_evicted_with_access_times_loaded_from_disk() -> Result<(), Error> {
    // Note these are swapped to ensure they aren't in numerical order.
    let digest1 = DigestInfo::try_new(HASH2, VALUE2.len())?;
    let digest2 = DigestInfo::try_new(HASH1, VALUE1.len())?;

    let content_path = make_temp_path("content_path");
    fs::create_dir_all(&content_path).await?;

    // Make the two files on disk before loading the store.
    let file1 = OsString::from(format!("{content_path}/{digest1}"));
    let file2 = OsString::from(format!("{content_path}/{digest2}"));
    write_file(&file1, VALUE1.as_bytes()).await?;
    write_file(&file2, VALUE2.as_bytes()).await?;
    set_file_atime(&file1, FileTime::from_unix_time(0, 0))?;
    set_file_atime(&file2, FileTime::from_unix_time(1, 0))?;

    // Load the existing store from disk.
    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new(&nativelink_config::stores::FilesystemStore {
            content_path,
            temp_path: make_temp_path("temp_path"),
            eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
                max_bytes: 0,
                max_seconds: 0,
                max_count: 1,
                evict_bytes: 0,
            }),
            ..Default::default()
        })
        .await?,
    );

    // This should exist and not have been evicted.
    store.get_file_entry_for_digest(&digest2).await?;
    // This should have been evicted.
    match store.get_file_entry_for_digest(&digest1).await {
        Ok(_) => panic!("Oldest file should have been evicted."),
        Err(error) => assert_eq!(error.code, Code::NotFound),
    }

    Ok(())
}

#[serial]
#[nativelink_test]
async fn eviction_drops_file_test() -> Result<(), Error> {
    let digest1 = DigestInfo::try_new(HASH1, VALUE1.len())?;

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new(&nativelink_config::stores::FilesystemStore {
            content_path: make_temp_path("content_path"),
            temp_path: make_temp_path("temp_path"),
            eviction_policy: None,
            ..Default::default()
        })
        .await?,
    );
    // Insert data into store.
    store.update_oneshot(digest1, VALUE1.into()).await?;

    let file_entry = store.get_file_entry_for_digest(&digest1).await?;
    file_entry
        .get_file_path_locked(move |path| async move {
            // Set atime to along time ago.
            set_file_atime(&path, FileTime::from_system_time(SystemTime::UNIX_EPOCH))?;

            // Check to ensure it was set to zero from previous command.
            assert_eq!(
                fs::metadata(&path).await?.accessed()?,
                SystemTime::UNIX_EPOCH
            );
            Ok(())
        })
        .await?;

    // Now touch digest1.
    let data = store.get_part_unchunked(digest1, 0, None).await?;
    assert_eq!(data, VALUE1.as_bytes());

    file_entry
        .get_file_path_locked(move |path| async move {
            // Ensure it was updated.
            assert!(fs::metadata(&path).await?.accessed()? > SystemTime::UNIX_EPOCH);
            Ok(())
        })
        .await?;

    Ok(())
}

// Test to ensure that if we are holding a reference to `FileEntry` and the contents are
// replaced, the `FileEntry` continues to use the old data.
// `FileEntry` file contents should be immutable for the lifetime of the object.
#[serial]
#[nativelink_test]
async fn digest_contents_replaced_continues_using_old_data() -> Result<(), Error> {
    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new(&nativelink_config::stores::FilesystemStore {
            content_path: make_temp_path("content_path"),
            temp_path: make_temp_path("temp_path"),
            eviction_policy: None,
            ..Default::default()
        })
        .await?,
    );
    // Insert data into store.
    store.update_oneshot(digest, VALUE1.into()).await?;
    let file_entry = store.get_file_entry_for_digest(&digest).await?;
    {
        // The file contents should equal our initial data.
        let mut reader = file_entry.read_file_part(0, u64::MAX).await?;
        let mut file_contents = String::new();
        reader
            .as_reader()
            .await?
            .read_to_string(&mut file_contents)
            .await?;
        assert_eq!(file_contents, VALUE1);
    }

    // Now replace the data.
    store.update_oneshot(digest, VALUE2.into()).await?;

    {
        // The file contents still equal our old data.
        let mut reader = file_entry.read_file_part(0, u64::MAX).await?;
        let mut file_contents = String::new();
        reader
            .as_reader()
            .await?
            .read_to_string(&mut file_contents)
            .await?;
        assert_eq!(file_contents, VALUE1);
    }

    Ok(())
}

#[serial]
#[nativelink_test]
async fn eviction_on_insert_calls_unref_once() -> Result<(), Error> {
    const SMALL_VALUE: &str = "01";
    const BIG_VALUE: &str = "0123";

    static UNREFED_DIGESTS: LazyLock<Mutex<Vec<DigestInfo>>> =
        LazyLock::new(|| Mutex::new(Vec::new()));
    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_unref<Fe: FileEntry>(file_entry: &Fe) {
            block_on(file_entry.get_file_path_locked(move |path_str| async move {
                let path = Path::new(&path_str);
                let digest =
                    digest_from_filename(path.file_name().unwrap().to_str().unwrap()).unwrap();
                UNREFED_DIGESTS.lock().push(digest);
                Ok(())
            }))
            .unwrap();
        }
    }

    let small_digest = DigestInfo::try_new(HASH1, SMALL_VALUE.len())?;
    let big_digest = DigestInfo::try_new(HASH1, BIG_VALUE.len())?;

    let store = Box::pin(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(
            &nativelink_config::stores::FilesystemStore {
                content_path: make_temp_path("content_path"),
                temp_path: make_temp_path("temp_path"),
                eviction_policy: Some(nativelink_config::stores::EvictionPolicy {
                    max_bytes: 5,
                    ..Default::default()
                }),
                block_size: 1,
                ..Default::default()
            },
        )
        .await?,
    );
    // Insert data into store.
    store
        .update_oneshot(small_digest, SMALL_VALUE.into())
        .await?;
    store.update_oneshot(big_digest, BIG_VALUE.into()).await?;

    {
        // Our first digest should have been unrefed exactly once.
        let unrefed_digests = UNREFED_DIGESTS.lock();
        assert_eq!(
            unrefed_digests.len(),
            1,
            "Expected exactly 1 unrefed digest"
        );
        assert_eq!(unrefed_digests[0], small_digest, "Expected digest to match");
    }

    Ok(())
}

#[serial]
#[nativelink_test]
#[allow(clippy::await_holding_refcell_ref)]
async fn rename_on_insert_fails_due_to_filesystem_error_proper_cleanup_happens() -> Result<(), Error>
{
    const INITIAL_CONTENT: &str = "hello";

    async fn wait_for_temp_file<Fut: Future<Output = Result<(), Error>>, F: Fn() -> Fut>(
        temp_path: &str,
        yield_fn: F,
    ) -> Result<fs::DirEntry, Error> {
        loop {
            yield_fn().await?;
            let (_permit, dir_handle) = fs::read_dir(&temp_path).await?.into_inner();
            let mut read_dir_stream = ReadDirStream::new(dir_handle);
            if let Some(dir_entry) = read_dir_stream.next().await {
                assert!(
                    read_dir_stream.next().await.is_none(),
                    "There should only be one file in temp directory"
                );
                let dir_entry = dir_entry?;
                {
                    // Some filesystems won't sync automatically, so force it.
                    let mut file_handle =
                        fs::open_file(dir_entry.path().into_os_string(), u64::MAX)
                            .await
                            .err_tip(|| "Failed to open temp file")?;
                    // We don't care if it fails, this is only best attempt.
                    let _ = file_handle
                        .as_reader()
                        .await?
                        .get_ref()
                        .as_ref()
                        .sync_all()
                        .await;
                }
                // Ensure we have written to the file too. This ensures we have an open file handle.
                // Failing to do this may result in the file existing, but the `update_fut` not actually
                // sending data to it yet.
                if dir_entry.metadata().await?.len() >= INITIAL_CONTENT.len() as u64 {
                    return Ok(dir_entry);
                }
            }
        }
        // Unreachable.
    }

    static FILE_DELETED_BARRIER: LazyLock<Arc<Barrier>> =
        LazyLock::new(|| Arc::new(Barrier::new(2)));

    struct LocalHooks {}
    impl FileEntryHooks for LocalHooks {
        fn on_drop<Fe: FileEntry>(_file_entry: &Fe) {
            background_spawn!("rename_on_insert_fails_due_to_filesystem_error_proper_cleanup_happens_local_hooks_on_drop", FILE_DELETED_BARRIER.wait());
        }
    }

    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;

    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Box::pin(
        FilesystemStore::<TestFileEntry<LocalHooks>>::new(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                eviction_policy: None,
                ..Default::default()
            },
        )
        .await?,
    );

    let (mut tx, rx) = make_buf_channel_pair();
    let update_fut = Arc::new(async_lock::Mutex::new(store.update(
        digest,
        rx,
        UploadSizeInfo::MaxSize(100),
    )));
    // This will process as much of the future as it can before it needs to pause.
    // Our temp file will be created and opened and ready to have contents streamed
    // to it.
    assert_eq!(poll!(&mut *update_fut.lock().await)?, Poll::Pending);
    tx.send(INITIAL_CONTENT.into()).await?;

    // Now we extract that temp file that is generated.
    wait_for_temp_file(&temp_path, || {
        let update_fut_clone = update_fut.clone();
        async move {
            // This will ensure we yield to our future and other potential spawns.
            tokio::task::yield_now().await;
            assert_eq!(poll!(&mut *update_fut_clone.lock().await)?, Poll::Pending);
            Ok(())
        }
    })
    .await?;

    // Now make it impossible for the file to be moved into the final path.
    // This will trigger an error on `rename()`.
    fs::remove_dir_all(&content_path).await?;

    // Because send_eof() waits for shutdown of the rx side, we cannot just await in this thread.
    background_spawn!(
        "rename_on_insert_fails_due_to_filesystem_error_proper_cleanup_happens_send_eof",
        async move {
            tx.send_eof().unwrap();
        },
    );

    // Now finish waiting on update(). This should reuslt in an error because we deleted our dest
    // folder.
    let update_result = &mut *update_fut.lock().await;
    assert!(
        update_result.await.is_err(),
        "Expected update to fail due to temp file being deleted before rename"
    );

    // Delete may happen on another thread, so wait for it.
    FILE_DELETED_BARRIER.wait().await;

    // Now it should have cleaned up its temp files.
    {
        // Ensure `temp_path` is empty.
        let (_permit, dir_handle) = fs::read_dir(&temp_path).await?.into_inner();
        let mut read_dir_stream = ReadDirStream::new(dir_handle);
        assert!(
            read_dir_stream.next().await.is_none(),
            "File found in temp_path after update() rename failure"
        );
    }

    // Finally ensure that our entry is not in the store.
    assert_eq!(
        store.has(digest).await?,
        None,
        "Entry should not be in store"
    );
    Ok(())
}

#[serial]
#[nativelink_test]
async fn get_part_timeout_test() -> Result<(), Error> {
    let large_value = "x".repeat(1024);
    let digest = DigestInfo::try_new(HASH1, large_value.len())?;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |_| sleep(Duration::ZERO),
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    store
        .update_oneshot(digest, large_value.clone().into())
        .await?;

    let (writer, mut reader) = make_buf_channel_pair();
    let store_clone = store.clone();
    let digest_clone = digest;

    let _drop_guard = spawn!("get_part_timeout_test_get", async move {
        store_clone.get(digest_clone, writer).await
    });

    let file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading bytes")?;

    assert_eq!(
        &file_data,
        large_value.as_bytes(),
        "Expected file content to match"
    );

    Ok(())
}

#[serial]
#[nativelink_test]
async fn get_part_is_zero_digest() -> Result<(), Error> {
    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |_| sleep(Duration::ZERO),
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    let store_clone = store.clone();
    let (mut writer, mut reader) = make_buf_channel_pair();

    let _drop_guard = spawn!("get_part_is_zero_digest_get_part", async move {
        let _ = store_clone
            .get_part(digest, &mut writer, 0, None)
            .await
            .err_tip(|| "Failed to get_part");
    });

    let file_data = reader
        .consume(Some(1024))
        .await
        .err_tip(|| "Error reading bytes")?;

    let empty_bytes = Bytes::new();
    assert_eq!(&file_data, &empty_bytes, "Expected file content to match");

    Ok(())
}

#[serial]
#[nativelink_test]
async fn has_with_results_on_zero_digests() -> Result<(), Error> {
    async fn wait_for_empty_content_file<
        Fut: Future<Output = Result<(), Error>>,
        F: Fn() -> Fut,
    >(
        content_path: &str,
        digest: DigestInfo,
        yield_fn: F,
    ) -> Result<(), Error> {
        loop {
            yield_fn().await?;

            let empty_digest_file_name = OsString::from(format!("{content_path}/{digest}"));

            let file_metadata = fs::metadata(empty_digest_file_name)
                .await
                .err_tip(|| "Failed to open content file")?;

            // Test that the empty digest file is created and contains an empty length.
            if file_metadata.is_file() && file_metadata.len() == 0 {
                return Ok(());
            }
        }
        // Unreachable.
    }

    let digest = DigestInfo::new(Sha256::new().finalize().into(), 0);
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let store = Arc::new(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |_| sleep(Duration::ZERO),
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    let keys = vec![digest.into()];
    let mut results = vec![None];
    let _ = store
        .has_with_results(&keys, &mut results)
        .await
        .err_tip(|| "Failed to get_part");
    assert_eq!(results, vec!(Some(0)));

    wait_for_empty_content_file(&content_path, digest, || async move {
        tokio::task::yield_now().await;
        Ok(())
    })
    .await?;

    Ok(())
}

/// Regression test for: https://github.com/TraceMachina/nativelink/issues/495.
#[serial]
#[nativelink_test(flavor = "multi_thread")]
async fn update_file_future_drops_before_rename() -> Result<(), Error> {
    // Mutex can be used to signal to the rename function to pause execution.
    static RENAME_REQUEST_PAUSE_MUX: async_lock::Mutex<()> = async_lock::Mutex::new(());
    // Boolean used to know if the rename function is currently paused.
    static RENAME_IS_PAUSED: AtomicBool = AtomicBool::new(false);

    let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;

    let content_path = make_temp_path("content_path");
    let store = Arc::pin(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: make_temp_path("temp_path"),
                eviction_policy: None,
                ..Default::default()
            },
            |_| sleep(Duration::ZERO),
            |from, to| {
                // If someone locked our mutex, it means we need to pause, so we
                // simply request a lock on the same mutex.
                if RENAME_REQUEST_PAUSE_MUX.try_lock().is_none() {
                    RENAME_IS_PAUSED.store(true, Ordering::Release);
                    while RENAME_REQUEST_PAUSE_MUX.try_lock().is_none() {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    RENAME_IS_PAUSED.store(false, Ordering::Release);
                }
                std::fs::rename(from, to)
            },
        )
        .await?,
    );

    // Populate our first store entry.
    let first_file_entry = {
        store.update_oneshot(digest, VALUE1.into()).await?;
        store.get_file_entry_for_digest(&digest).await?
    };

    // 1. Request the next rename function to block.
    // 2. Request to replace our data.
    // 3. When we are certain that our rename function is paused, drop
    //    the replace/update future.
    // 4. Then drop the lock.
    {
        let rename_pause_request_lock = RENAME_REQUEST_PAUSE_MUX.lock().await;
        let mut update_fut = store.update_oneshot(digest, VALUE2.into()).boxed();

        loop {
            // Try to advance our update future.
            assert_eq!(poll!(&mut update_fut), Poll::Pending);

            // Once we are sure the rename fuction is paused break.
            if RENAME_IS_PAUSED.load(Ordering::Acquire) {
                break;
            }
            // Give a little time for background/kernel threads to run.
            sleep(Duration::from_millis(1)).await;
        }
        // Writing these out explicitly so users know this is what we are testing.
        // Note: The order they are dropped matters.
        drop(update_fut);
        drop(rename_pause_request_lock);
    }
    // Grab the newly inserted item in our store.
    let new_file_entry = store.get_file_entry_for_digest(&digest).await?;
    assert!(
        !Arc::ptr_eq(&first_file_entry, &new_file_entry),
        "Expected file entries to not be the same"
    );

    // Ensure the entry we inserted was properly flagged as moved (from temp -> content dir).
    new_file_entry
        .get_file_path_locked(move |file_path| async move {
            assert_eq!(
                file_path,
                OsString::from(format!("{content_path}/{digest}"))
            );
            Ok(())
        })
        .await?;

    Ok(())
}

// Ensure that get_file_size() returns the correct number
// ceil(content length / block_size) * block_size
// assume block size 4K
// 1B data size = 4K size on disk
// 5K data size = 8K size on disk
#[serial]
#[nativelink_test]
async fn get_file_size_uses_block_size() -> Result<(), Error> {
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let value_1kb: String = "x".repeat(1024);
    let value_5kb: String = "xabcd".repeat(1024);

    let digest_1kb = DigestInfo::try_new(HASH1, value_1kb.len())?;
    let digest_5kb = DigestInfo::try_new(HASH2, value_5kb.len())?;

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |_| sleep(Duration::ZERO),
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    store.update_oneshot(digest_1kb, value_1kb.into()).await?;
    let short_entry = store.get_file_entry_for_digest(&digest_1kb).await?;
    assert_eq!(short_entry.size_on_disk(), 4 * 1024);

    store.update_oneshot(digest_5kb, value_5kb.into()).await?;
    let long_entry = store.get_file_entry_for_digest(&digest_5kb).await?;
    assert_eq!(long_entry.size_on_disk(), 8 * 1024);
    Ok(())
}

#[serial]
#[nativelink_test]
async fn update_with_whole_file_closes_file() -> Result<(), Error> {
    let mut permits = vec![];
    // Grab all permits to ensure only 1 permit is available.
    {
        wait_for_no_open_files().await?;
        while fs::OPEN_FILE_SEMAPHORE.available_permits() > 1 {
            permits.push(fs::get_permit().await);
        }
        assert_eq!(
            fs::OPEN_FILE_SEMAPHORE.available_permits(),
            1,
            "Expected 1 permit to be available"
        );
    }
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let value = "x".repeat(1024);

    let digest = DigestInfo::try_new(HASH1, value.len())?;

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new(&nativelink_config::stores::FilesystemStore {
            content_path: content_path.clone(),
            temp_path: temp_path.clone(),
            read_buffer_size: 1,
            ..Default::default()
        })
        .await?,
    );
    store.update_oneshot(digest, value.clone().into()).await?;

    let mut file = fs::create_file(OsString::from(format!("{temp_path}/dummy_file"))).await?;
    {
        let writer = file.as_writer().await?;
        writer.write_all(value.as_bytes()).await?;
        writer.as_mut().sync_all().await?;
        writer.seek(tokio::io::SeekFrom::Start(0)).await?;
    }

    store
        .update_with_whole_file(digest, file, UploadSizeInfo::ExactSize(value.len() as u64))
        .await?;
    Ok(())
}

#[serial]
#[nativelink_test]
async fn update_with_whole_file_slow_path_when_low_file_descriptors() -> Result<(), Error> {
    let mut permits = vec![];
    // Grab all permits to ensure only 1 permit is available.
    {
        wait_for_no_open_files().await?;
        while fs::OPEN_FILE_SEMAPHORE.available_permits() > 1 {
            permits.push(fs::get_permit().await);
        }
        assert_eq!(
            fs::OPEN_FILE_SEMAPHORE.available_permits(),
            1,
            "Expected 1 permit to be available"
        );
    }

    let value = "x".repeat(1024);

    let digest = DigestInfo::try_new(HASH1, value.len())?;

    let store = FastSlowStore::new(
        // Note: The config is not needed for this test, so use dummy data.
        &nativelink_config::stores::FastSlowStore {
            fast: nativelink_config::stores::StoreConfig::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
            slow: nativelink_config::stores::StoreConfig::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
        },
        Store::new(
            FilesystemStore::<FileEntryImpl>::new(&nativelink_config::stores::FilesystemStore {
                content_path: make_temp_path("content_path"),
                temp_path: make_temp_path("temp_path"),
                read_buffer_size: 1,
                ..Default::default()
            })
            .await?,
        ),
        Store::new(
            FilesystemStore::<FileEntryImpl>::new(&nativelink_config::stores::FilesystemStore {
                content_path: make_temp_path("content_path1"),
                temp_path: make_temp_path("temp_path1"),
                read_buffer_size: 1,
                ..Default::default()
            })
            .await?,
        ),
    );
    store.update_oneshot(digest, value.clone().into()).await?;

    let temp_path = make_temp_path("temp_path2");
    fs::create_dir_all(&temp_path).await?;
    let mut file = fs::create_file(OsString::from(format!("{temp_path}/dummy_file"))).await?;
    {
        let writer = file.as_writer().await?;
        writer.write_all(value.as_bytes()).await?;
        writer.as_mut().sync_all().await?;
        writer.seek(tokio::io::SeekFrom::Start(0)).await?;
    }

    store
        .update_with_whole_file(digest, file, UploadSizeInfo::ExactSize(value.len() as u64))
        .await?;
    Ok(())
}

// Ensure that update_with_whole_file() moves the file without making a copy.
#[cfg(target_family = "unix")]
#[serial]
#[nativelink_test]
async fn update_with_whole_file_uses_same_inode() -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    let content_path = make_temp_path("content_path");
    let temp_path = make_temp_path("temp_path");

    let value: String = "x".repeat(1024);

    let digest = DigestInfo::try_new(HASH1, value.len())?;

    let store = Box::pin(
        FilesystemStore::<FileEntryImpl>::new_with_timeout_and_rename_fn(
            &nativelink_config::stores::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                read_buffer_size: 1,
                ..Default::default()
            },
            |_| sleep(Duration::ZERO),
            |from, to| std::fs::rename(from, to),
        )
        .await?,
    );

    let mut file = fs::create_file(OsString::from(format!("{temp_path}/dummy_file"))).await?;
    let original_inode = file
        .as_reader()
        .await?
        .get_ref()
        .as_ref()
        .metadata()
        .await?
        .ino();

    let result = store
        .update_with_whole_file(digest, file, UploadSizeInfo::ExactSize(value.len() as u64))
        .await?;
    assert!(
        result.is_none(),
        "Expected filesystem store to consume the file"
    );

    let expected_file_name = OsString::from(format!("{content_path}/{digest}"));
    let new_inode = fs::create_file(expected_file_name)
        .await?
        .as_reader()
        .await?
        .get_ref()
        .as_ref()
        .metadata()
        .await?
        .ino();
    assert_eq!(
        original_inode, new_inode,
        "Expected the same inode for the file"
    );

    Ok(())
}
