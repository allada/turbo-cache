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

use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use maplit::hashmap;
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;
use nativelink_proto::build::bazel::remote::execution::v2::{
    batch_read_blobs_response, batch_update_blobs_request, batch_update_blobs_response, compressor,
    digest_function, BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, Digest, Directory, DirectoryNode, FindMissingBlobsRequest,
    GetTreeRequest, GetTreeResponse, NodeProperties,
};
use nativelink_proto::google::rpc::Status as GrpcStatus;
use nativelink_service::cas_server::CasServer;
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::default_store_factory::make_and_add_store_to_manager;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{StoreKey, StoreLike};
use pretty_assertions::assert_eq;
use prost_types::Timestamp;
use tonic::{Code, Request};

const INSTANCE_NAME: &str = "foo_instance_name";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const HASH2: &str = "9993456789abcdef000000000000000000000000000000000123456789abc999";
const HASH3: &str = "7773456789abcdef000000000000000000000000000000000123456789abc777";
const BAD_HASH: &str = "BAD_HASH";

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    make_and_add_store_to_manager(
        "main_cas",
        &StoreSpec::memory(MemorySpec::default()),
        &store_manager,
        None,
    )
    .await?;

    Ok(store_manager)
}

fn make_cas_server(store_manager: &StoreManager) -> Result<CasServer, Error> {
    CasServer::new(
        &hashmap! {
            "foo_instance_name".to_string() => nativelink_config::cas_server::CasStoreConfig{
                cas_store: "main_cas".to_string(),
            }
        },
        store_manager,
    )
}

#[nativelink_test]
async fn empty_store() -> Result<(), Box<dyn std::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;

    let raw_response = cas_server
        .find_missing_blobs(Request::new(FindMissingBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digests: vec![Digest {
                hash: HASH1.to_string(),
                size_bytes: 0,
            }],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    assert!(raw_response.is_ok());
    let response = raw_response.unwrap().into_inner();
    assert_eq!(response.missing_blob_digests.len(), 1);
    Ok(())
}

#[nativelink_test]
async fn store_one_item_existence() -> Result<(), Box<dyn std::error::Error>> {
    const VALUE: &str = "1";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    store
        .update_oneshot(DigestInfo::try_new(HASH1, VALUE.len())?, VALUE.into())
        .await?;
    let raw_response = cas_server
        .find_missing_blobs(Request::new(FindMissingBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digests: vec![Digest {
                hash: HASH1.to_string(),
                size_bytes: VALUE.len() as i64,
            }],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    assert!(raw_response.is_ok());
    let response = raw_response.unwrap().into_inner();
    assert_eq!(response.missing_blob_digests.len(), 0); // All items should have been found.
    Ok(())
}

#[nativelink_test]
async fn has_three_requests_one_bad_hash() -> Result<(), Box<dyn std::error::Error>> {
    const VALUE: &str = "1";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    store
        .update_oneshot(DigestInfo::try_new(HASH1, VALUE.len())?, VALUE.into())
        .await?;
    let raw_response = cas_server
        .find_missing_blobs(Request::new(FindMissingBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            blob_digests: vec![
                Digest {
                    hash: HASH1.to_string(),
                    size_bytes: VALUE.len() as i64,
                },
                Digest {
                    hash: BAD_HASH.to_string(),
                    size_bytes: VALUE.len() as i64,
                },
                Digest {
                    hash: HASH1.to_string(),
                    size_bytes: VALUE.len() as i64,
                },
            ],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    let error = raw_response.unwrap_err();
    assert!(
        error.to_string().contains("Invalid sha256 hash: BAD_HASH"),
        "'Invalid sha256 hash: BAD_HASH' not found in: {error:?}"
    );
    Ok(())
}

#[nativelink_test]
async fn update_existing_item() -> Result<(), Box<dyn std::error::Error>> {
    const VALUE1: &str = "1";
    const VALUE2: &str = "2";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let digest = Digest {
        hash: HASH1.to_string(),
        size_bytes: VALUE2.len() as i64,
    };

    store
        .update_oneshot(DigestInfo::try_new(HASH1, VALUE1.len())?, VALUE1.into())
        .await
        .expect("Update should have succeeded");

    let raw_response = cas_server
        .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
            instance_name: INSTANCE_NAME.to_string(),
            requests: vec![batch_update_blobs_request::Request {
                digest: Some(digest.clone()),
                data: VALUE2.into(),
                compressor: compressor::Value::Identity.into(),
            }],
            digest_function: digest_function::Value::Sha256.into(),
        }))
        .await;
    assert!(raw_response.is_ok());
    assert_eq!(
        raw_response.unwrap().into_inner(),
        BatchUpdateBlobsResponse {
            responses: vec![batch_update_blobs_response::Response {
                digest: Some(digest),
                status: Some(GrpcStatus {
                    code: 0, // Status Ok.
                    message: String::new(),
                    details: vec![],
                }),
            },],
        }
    );
    let new_data = store
        .get_part_unchunked(DigestInfo::try_new(HASH1, VALUE1.len())?, 0, None)
        .await
        .expect("Get should have succeeded");
    assert_eq!(
        new_data,
        VALUE2.as_bytes(),
        "Expected store to have been updated to new value"
    );
    Ok(())
}

#[nativelink_test]
async fn batch_read_blobs_read_two_blobs_success_one_fail() -> Result<(), Box<dyn std::error::Error>>
{
    const VALUE1: &str = "1";
    const VALUE2: &str = "23";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    let digest1 = Digest {
        hash: HASH1.to_string(),
        size_bytes: VALUE1.len() as i64,
    };
    let digest2 = Digest {
        hash: HASH2.to_string(),
        size_bytes: VALUE2.len() as i64,
    };
    {
        // Insert dummy data.
        store
            .update_oneshot(DigestInfo::try_new(HASH1, VALUE1.len())?, VALUE1.into())
            .await
            .expect("Update should have succeeded");
        store
            .update_oneshot(DigestInfo::try_new(HASH2, VALUE2.len())?, VALUE2.into())
            .await
            .expect("Update should have succeeded");
    }
    {
        // Read two blobs and additional blob should come back not found.
        let digest3 = Digest {
            hash: HASH3.to_string(),
            size_bytes: 3,
        };
        let raw_response = cas_server
            .batch_read_blobs(Request::new(BatchReadBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                digests: vec![digest1.clone(), digest2.clone(), digest3.clone()],
                acceptable_compressors: vec![compressor::Value::Identity.into()],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response.unwrap().into_inner(),
            BatchReadBlobsResponse {
                responses: vec![
                    batch_read_blobs_response::Response {
                        digest: Some(digest1),
                        data: VALUE1.into(),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                        compressor: compressor::Value::Identity.into(),
                    },
                    batch_read_blobs_response::Response {
                        digest: Some(digest2),
                        data: VALUE2.into(),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                        compressor: compressor::Value::Identity.into(),
                    },
                    batch_read_blobs_response::Response {
                        digest: Some(digest3.clone()),
                        data: vec![].into(),
                        status: Some(GrpcStatus {
                            code: Code::NotFound as i32,
                            message: format!(
                                "Key {:?} not found",
                                StoreKey::from(DigestInfo::try_from(digest3)?)
                            ),
                            details: vec![],
                        }),
                        compressor: compressor::Value::Identity.into(),
                    }
                ],
            }
        );
    }
    Ok(())
}

struct SetupDirectoryResult {
    root_directory: Directory,
    root_directory_digest_info: DigestInfo,
    sub_directories: Vec<Directory>,
    sub_directory_digest_infos: Vec<DigestInfo>,
}
async fn setup_directory_structure(
    store_pinned: Pin<&impl StoreLike>,
) -> Result<SetupDirectoryResult, Error> {
    // Set up 5 sub-directories.
    const SUB_DIRECTORIES_LENGTH: i32 = 5;
    let mut sub_directory_nodes: Vec<DirectoryNode> = vec![];
    let mut sub_directories: Vec<Directory> = vec![];
    let mut sub_directory_digest_infos: Vec<DigestInfo> = vec![];

    for i in 0..SUB_DIRECTORIES_LENGTH {
        let sub_directory: Directory = Directory {
            files: vec![],
            directories: vec![],
            symlinks: vec![],
            node_properties: Some(NodeProperties {
                properties: vec![],
                mtime: Some(Timestamp {
                    seconds: i64::from(i),
                    nanos: 0,
                }),
                unix_mode: Some(0o755),
            }),
        };
        let sub_directory_digest_info: DigestInfo = serialize_and_upload_message(
            &sub_directory,
            store_pinned,
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        sub_directory_digest_infos.push(sub_directory_digest_info);
        sub_directory_nodes.push(DirectoryNode {
            name: format!("sub_directory_{i}"),
            digest: Some(sub_directory_digest_info.into()),
        });
        sub_directories.push(sub_directory);
    }

    // Set up a root directory.
    let root_directory: Directory = Directory {
        files: vec![],
        directories: sub_directory_nodes,
        symlinks: vec![],
        node_properties: None,
    };
    let root_directory_digest_info: DigestInfo = serialize_and_upload_message(
        &root_directory,
        store_pinned,
        &mut DigestHasherFunc::Sha256.hasher(),
    )
    .await?;

    Ok(SetupDirectoryResult {
        root_directory,
        root_directory_digest_info,
        sub_directories,
        sub_directory_digest_infos,
    })
}

#[nativelink_test]
async fn get_tree_read_directories_without_paging() -> Result<(), Box<dyn std::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Setup directory structure.
    let SetupDirectoryResult {
        root_directory,
        root_directory_digest_info,
        sub_directories,
        sub_directory_digest_infos: _,
    } = setup_directory_structure(store.as_pin()).await?;

    // Must work when paging is disabled ( `page_size` is 0 ).
    // It reads all directories at once.

    // First verify that using an empty page token is treated as if the client had sent the root
    // digest.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 0,
                page_token: String::new(),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![
                    root_directory.clone(),
                    sub_directories[0].clone(),
                    sub_directories[1].clone(),
                    sub_directories[2].clone(),
                    sub_directories[3].clone(),
                    sub_directories[4].clone()
                ],
                next_page_token: String::new()
            }]
        );
    }

    // Also verify that sending the root digest returns the entire tree as well.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 0,
                page_token: format!("{root_directory_digest_info}"),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![
                    root_directory.clone(),
                    sub_directories[0].clone(),
                    sub_directories[1].clone(),
                    sub_directories[2].clone(),
                    sub_directories[3].clone(),
                    sub_directories[4].clone()
                ],
                next_page_token: String::new()
            }]
        );
    }

    Ok(())
}

#[nativelink_test]
async fn get_tree_read_directories_with_paging() -> Result<(), Box<dyn std::error::Error>> {
    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;
    let store = store_manager.get_store("main_cas").unwrap();

    // Setup directory structure.
    let SetupDirectoryResult {
        root_directory,
        root_directory_digest_info,
        sub_directories,
        sub_directory_digest_infos,
    } = setup_directory_structure(store.as_pin()).await?;

    // Must work when paging is enabled ( `page_size` is 2 ).
    // First, it reads `root_directory` and `sub_directory[0]`.
    // Then, it reads `sub_directory[1]` and `sub_directory[2]`.
    // Finally, it reads `sub_directory[3]` and `sub_directory[4]`.

    // First, verify that an empty initial page token is treated as if the client had sent the
    // root digest and respects the page size.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: String::new(),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![root_directory.clone(), sub_directories[0].clone()],
                next_page_token: format!("{}", sub_directory_digest_infos[1]),
            }]
        );
    }

    // Also verify that sending the root digest as the page token is treated as paging from the
    // beginning and respects page size.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!("{root_directory_digest_info}"),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![root_directory.clone(), sub_directories[0].clone()],
                next_page_token: format!("{}", sub_directory_digest_infos[1]),
            }]
        );
    }

    // Verify that paging from a non-initial page token will return the expected content.
    {
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!("{}", sub_directory_digest_infos[1]),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![sub_directories[1].clone(), sub_directories[2].clone()],
                next_page_token: format!("{}", sub_directory_digest_infos[3]),
            }]
        );

        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!("{}", sub_directory_digest_infos[3]),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![sub_directories[3].clone(), sub_directories[4].clone()],
                next_page_token: String::new(),
            }]
        );
    }

    Ok(())
}

#[nativelink_test]
async fn batch_update_blobs_two_items_existence_with_third_missing(
) -> Result<(), Box<dyn std::error::Error>> {
    const VALUE1: &str = "1";
    const VALUE2: &str = "23";

    let store_manager = make_store_manager().await?;
    let cas_server = make_cas_server(&store_manager)?;

    let digest1 = Digest {
        hash: HASH1.to_string(),
        size_bytes: VALUE1.len() as i64,
    };
    let digest2 = Digest {
        hash: HASH2.to_string(),
        size_bytes: VALUE2.len() as i64,
    };

    {
        // Send update to insert two entries into backend.
        let raw_response = cas_server
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                requests: vec![
                    batch_update_blobs_request::Request {
                        digest: Some(digest1.clone()),
                        data: VALUE1.into(),
                        compressor: compressor::Value::Identity.into(),
                    },
                    batch_update_blobs_request::Request {
                        digest: Some(digest2.clone()),
                        data: VALUE2.into(),
                        compressor: compressor::Value::Identity.into(),
                    },
                ],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response.unwrap().into_inner(),
            BatchUpdateBlobsResponse {
                responses: vec![
                    batch_update_blobs_response::Response {
                        digest: Some(digest1),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                    },
                    batch_update_blobs_response::Response {
                        digest: Some(digest2),
                        status: Some(GrpcStatus {
                            code: 0, // Status Ok.
                            message: String::new(),
                            details: vec![],
                        }),
                    }
                ],
            }
        );
    }
    {
        // Query the backend for inserted entries plus one that is not
        // present and ensure it only returns the one that is missing.
        let missing_digest = Digest {
            hash: HASH3.to_string(),
            size_bytes: 1,
        };
        let raw_response = cas_server
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                blob_digests: vec![
                    Digest {
                        hash: HASH1.to_string(),
                        size_bytes: VALUE1.len() as i64,
                    },
                    missing_digest.clone(),
                    Digest {
                        hash: HASH2.to_string(),
                        size_bytes: VALUE2.len() as i64,
                    },
                ],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        let response = raw_response.unwrap().into_inner();
        assert_eq!(response.missing_blob_digests, vec![missing_digest]);
    }
    Ok(())
}
