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
use std::ffi::OsString;

use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::{fs, DigestInfo};
use nativelink_util::store_trait::{StoreLike, UploadSizeInfo};
use pretty_assertions::assert_eq;
use rand::{thread_rng, Rng};
use tokio::io::AsyncWriteExt;

/// Get temporary path from either `TEST_TMPDIR` or best effort temp directory if
/// not set.
async fn make_temp_path(data: &str) -> OsString {
    let dir = format!(
        "{}/{}",
        env::var("TEST_TMPDIR").unwrap_or(env::temp_dir().to_str().unwrap().to_string()),
        thread_rng().gen::<u64>(),
    );
    fs::create_dir_all(&dir).await.unwrap();
    OsString::from(format!("{dir}/{data}"))
}

const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const HASH1_SIZE: i64 = 147;

// Regression test for bug created when implementing ResumeableFileSlot
// where the timeout() success condition was breaking out of the outer
// loop resulting in the file always being created with <= 4096 bytes.
#[nativelink_test]
async fn upload_file_to_store_with_large_file() -> Result<(), Error> {
    let filepath = make_temp_path("test.txt").await;
    let expected_data = vec![0x88; 1024 * 1024]; // 1MB.
    let store = MemoryStore::new(&nativelink_config::stores::MemorySpec::default());
    let digest = DigestInfo::try_new(HASH1, HASH1_SIZE)?; // Dummy hash data.
    {
        // Write 1MB of 0x88s to the file.
        let mut file = tokio::fs::File::create(&filepath)
            .await
            .err_tip(|| "Could not open file")?;
        file.write_all(&expected_data)
            .await
            .err_tip(|| "Could not write to file")?;
        file.flush().await.err_tip(|| "Could not flush file")?;
        file.sync_all().await.err_tip(|| "Could not sync file")?;
    }
    {
        // Upload our file.
        let resumeable_file = fs::open_file(filepath, u64::MAX).await?;
        store
            .update_with_whole_file(
                digest,
                resumeable_file,
                UploadSizeInfo::ExactSize(expected_data.len() as u64),
            )
            .await?;
    }
    {
        // Check to make sure the file was saved correctly to the store.
        let store_data = store.get_part_unchunked(digest, 0, None).await?;
        assert_eq!(store_data.len(), expected_data.len());
        assert_eq!(store_data, expected_data);
    }
    Ok(())
}
