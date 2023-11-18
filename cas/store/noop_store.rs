// Copyright 2022 The Native Link Authors. All rights reserved.
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

use async_trait::async_trait;

use buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use common::DigestInfo;
use error::{make_err, Code, Error, ResultExt};
use traits::{StoreTrait, UploadSizeInfo};

#[derive(Default)]
pub struct NoopStore;

impl NoopStore {
    pub fn new() -> Self {
        NoopStore {}
    }
}

#[async_trait]
impl StoreTrait for NoopStore {
    async fn has_with_results(
        self: Pin<&Self>,
        _digests: &[DigestInfo],
        results: &mut [Option<usize>],
    ) -> Result<(), Error> {
        results.iter_mut().for_each(|r| *r = None);
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        _digest: DigestInfo,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        // We need to drain the reader to avoid the writer complaining that we dropped
        // the connection prematurely.
        loop {
            if reader.recv().await.err_tip(|| "In NoopStore::update")?.is_empty() {
                break; // EOF.
            }
        }
        Ok(())
    }

    async fn get_part_ref(
        self: Pin<&Self>,
        _digest: DigestInfo,
        _writer: &mut DropCloserWriteHalf,
        _offset: usize,
        _length: Option<usize>,
    ) -> Result<(), Error> {
        Err(make_err!(Code::NotFound, "Not found in noop store"))
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any + Send> {
        Box::new(self)
    }
}
