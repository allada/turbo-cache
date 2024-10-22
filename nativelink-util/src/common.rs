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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::io::{Cursor, Write};
use std::ops::{Deref, DerefMut};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use nativelink_error::{make_input_err, Error, ResultExt};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use prost::Message;
use serde::de::Visitor;
use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tracing::{event, Level};

pub use crate::fs;

#[derive(Default, Clone, Copy, Eq, PartialEq, Hash)]
#[repr(C)]
pub struct DigestInfo {
    /// Raw hash in packed form.
    packed_hash: PackedHash,

    /// Possibly the size of the digest in bytes.
    size_bytes: u64,
}

impl MetricsComponent for DigestInfo {
    fn publish(
        &self,
        _kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        format!("{self}").publish(MetricKind::String, field_metadata)
    }
}

impl DigestInfo {
    pub const fn new(packed_hash: [u8; 32], size_bytes: u64) -> Self {
        DigestInfo {
            size_bytes,
            packed_hash: PackedHash(packed_hash),
        }
    }

    pub fn try_new<T>(hash: &str, size_bytes: T) -> Result<Self, Error>
    where
        T: TryInto<u64> + std::fmt::Display + Copy,
    {
        let packed_hash =
            PackedHash::from_hex(hash).err_tip(|| format!("Invalid sha256 hash: {hash}"))?;
        let size_bytes = size_bytes
            .try_into()
            .map_err(|_| make_input_err!("Could not convert {} into u64", size_bytes))?;
        // The proto `Digest` takes an i64, so to keep compatibility
        // we only allow sizes that can fit into an i64.
        if size_bytes > i64::MAX as u64 {
            return Err(make_input_err!(
                "Size bytes is too large: {} - max: {}",
                size_bytes,
                i64::MAX
            ));
        }
        Ok(DigestInfo {
            packed_hash,
            size_bytes,
        })
    }

    pub const fn zero_digest() -> DigestInfo {
        DigestInfo {
            size_bytes: 0,
            packed_hash: PackedHash::new(),
        }
    }

    pub const fn packed_hash(&self) -> &PackedHash {
        &self.packed_hash
    }

    pub fn set_packed_hash(&mut self, packed_hash: [u8; 32]) {
        self.packed_hash = PackedHash(packed_hash);
    }

    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns a struct that can turn the `DigestInfo` into a string.
    const fn stringifier(&self) -> DigestStackStringifier<'_> {
        DigestStackStringifier::new(self)
    }
}

/// Counts the number of digits a number needs if it were to be
/// converted to a string.
const fn count_digits(mut num: u64) -> usize {
    let mut count = 0;
    while num != 0 {
        count += 1;
        num /= 10;
    }
    count
}

/// An optimized version of a function that can convert a `DigestInfo`
/// into a str on the stack.
struct DigestStackStringifier<'a> {
    digest: &'a DigestInfo,
    /// Buffer that can hold the string representation of the `DigestInfo`.
    /// - Hex is '2 * sizeof(PackedHash)'.
    /// - Digits can be at most `count_digits(u64::MAX)`.
    /// - We also have a hyphen separator.
    buf: [u8; std::mem::size_of::<PackedHash>() * 2 + count_digits(u64::MAX) + 1],
}

impl<'a> DigestStackStringifier<'a> {
    const fn new(digest: &'a DigestInfo) -> Self {
        DigestStackStringifier {
            digest,
            buf: [b'-'; std::mem::size_of::<PackedHash>() * 2 + count_digits(u64::MAX) + 1],
        }
    }

    fn as_str(&mut self) -> Result<&str, Error> {
        // Populate the buffer and return the amount of bytes written
        // to the buffer.
        let len = {
            let mut cursor = Cursor::new(&mut self.buf[..]);
            let hex = self.digest.packed_hash.to_hex().map_err(|e| {
                make_input_err!(
                    "Could not convert PackedHash to hex - {e:?} - {:?}",
                    self.digest
                )
            })?;
            cursor
                .write_all(&hex)
                .err_tip(|| format!("Could not write hex to buffer - {hex:?} - {hex:?}",))?;
            // Note: We already have a hyphen at this point because we
            // initialized the buffer with hyphens.
            cursor.advance(1);
            cursor
                .write_fmt(format_args!("{}", self.digest.size_bytes()))
                .err_tip(|| format!("Could not write size_bytes to buffer - {hex:?}",))?;
            usize::try_from(cursor.position()).expect("Could not convert usize to u64")
        };
        // Convert the buffer into utf8 string.
        std::str::from_utf8(&self.buf[..len]).map_err(|e| {
            make_input_err!(
                "Could not convert [u8] to string - {} - {:?} - {:?}",
                self.digest,
                self.buf,
                e,
            )
        })
    }
}

/// Custom serializer for `DigestInfo` because the default Serializer
/// would try to encode the data as a byte array, but we use {hex}-{size}.
impl Serialize for DigestInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut stringifier = self.stringifier();
        serializer.serialize_str(
            stringifier
                .as_str()
                .err_tip(|| "During serialization of DigestInfo")
                .map_err(S::Error::custom)?,
        )
    }
}

/// Custom deserializer for `DigestInfo` becaues the default Deserializer
/// would try to decode the data as a byte array, but we use {hex}-{size}.
impl<'de> Deserialize<'de> for DigestInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DigestInfoVisitor;
        impl<'a> Visitor<'a> for DigestInfoVisitor {
            type Value = DigestInfo;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string representing a DigestInfo")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let Some((hash, size)) = s.split_once('-') else {
                    return Err(E::custom(
                        "Invalid DigestInfo format, expected '-' separator",
                    ));
                };
                let size_bytes = size
                    .parse::<u64>()
                    .map_err(|e| E::custom(format!("Could not parse size_bytes: {e:?}")))?;
                DigestInfo::try_new(hash, size_bytes)
                    .map_err(|e| E::custom(format!("Could not create DigestInfo: {e:?}")))
            }
        }
        deserializer.deserialize_str(DigestInfoVisitor)
    }
}

impl fmt::Display for DigestInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut stringifier = self.stringifier();
        f.write_str(
            stringifier
                .as_str()
                .err_tip(|| "During serialization of DigestInfo")
                .map_err(|e| {
                    event!(
                        Level::ERROR,
                        "Could not convert DigestInfo to string - {e:?}"
                    );
                    fmt::Error
                })?,
        )
    }
}

impl fmt::Debug for DigestInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut stringifier = self.stringifier();
        match stringifier.as_str() {
            Ok(s) => f.debug_tuple("DigestInfo").field(&s).finish(),
            Err(e) => {
                event!(
                    Level::ERROR,
                    "Could not convert DigestInfo to string - {e:?}"
                );
                Err(fmt::Error)
            }
        }
    }
}

impl Ord for DigestInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.packed_hash
            .cmp(&other.packed_hash)
            .then_with(|| self.size_bytes.cmp(&other.size_bytes))
    }
}

impl PartialOrd for DigestInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<Digest> for DigestInfo {
    type Error = Error;

    fn try_from(digest: Digest) -> Result<Self, Self::Error> {
        let packed_hash = PackedHash::from_hex(&digest.hash)
            .err_tip(|| format!("Invalid sha256 hash: {}", digest.hash))?;
        let size_bytes = digest
            .size_bytes
            .try_into()
            .map_err(|_| make_input_err!("Could not convert {} into u64", digest.size_bytes))?;
        Ok(DigestInfo {
            packed_hash,
            size_bytes,
        })
    }
}

impl TryFrom<&Digest> for DigestInfo {
    type Error = Error;

    fn try_from(digest: &Digest) -> Result<Self, Self::Error> {
        let packed_hash = PackedHash::from_hex(&digest.hash)
            .err_tip(|| format!("Invalid sha256 hash: {}", digest.hash))?;
        let size_bytes = digest
            .size_bytes
            .try_into()
            .map_err(|_| make_input_err!("Could not convert {} into u64", digest.size_bytes))?;
        Ok(DigestInfo {
            packed_hash,
            size_bytes,
        })
    }
}

impl From<DigestInfo> for Digest {
    fn from(val: DigestInfo) -> Self {
        Digest {
            hash: val.packed_hash.to_string(),
            size_bytes: val.size_bytes.try_into().unwrap_or_else(|e| {
                event!(
                    Level::ERROR,
                    "Could not convert {} into u64 - {e:?}",
                    val.size_bytes
                );
                // This is a placeholder value that can help a user identify
                // that the conversion failed.
                -255
            }),
        }
    }
}

impl From<&DigestInfo> for Digest {
    fn from(val: &DigestInfo) -> Self {
        Digest {
            hash: val.packed_hash.to_string(),
            size_bytes: val.size_bytes.try_into().unwrap_or_else(|e| {
                event!(
                    Level::ERROR,
                    "Could not convert {} into u64 - {e:?}",
                    val.size_bytes
                );
                // This is a placeholder value that can help a user identify
                // that the conversion failed.
                -255
            }),
        }
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct PackedHash([u8; 32]);

const SIZE_OF_PACKED_HASH: usize = 32;
impl PackedHash {
    const fn new() -> Self {
        PackedHash([0; SIZE_OF_PACKED_HASH])
    }

    fn from_hex(hash: &str) -> Result<Self, Error> {
        let mut packed_hash = [0u8; 32];
        hex::decode_to_slice(hash, &mut packed_hash)
            .map_err(|e| make_input_err!("Invalid sha256 hash: {hash} - {e:?}"))?;
        Ok(PackedHash(packed_hash))
    }

    /// Converts the packed hash into a hex string.
    #[inline]
    fn to_hex(self) -> Result<[u8; SIZE_OF_PACKED_HASH * 2], fmt::Error> {
        let mut hash = [0u8; SIZE_OF_PACKED_HASH * 2];
        hex::encode_to_slice(self.0, &mut hash).map_err(|e| {
            event!(
                Level::ERROR,
                "Could not convert PackedHash to hex - {e:?} - {:?}",
                self.0
            );
            fmt::Error
        })?;
        Ok(hash)
    }
}

impl fmt::Display for PackedHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hash = self.to_hex()?;
        match std::str::from_utf8(&hash) {
            Ok(hash) => f.write_str(hash)?,
            Err(_) => f.write_str(&format!("Could not convert hash to utf8 {:?}", self.0))?,
        }
        Ok(())
    }
}

impl Deref for PackedHash {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PackedHash {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Simple utility trait that makes it easier to apply `.try_map` to Vec.
// This will convert one vector into another vector with a different type.
pub trait VecExt<T> {
    fn try_map<F, U>(self, f: F) -> Result<Vec<U>, Error>
    where
        Self: Sized,
        F: (std::ops::Fn(T) -> Result<U, Error>) + Sized;
}

impl<T> VecExt<T> for Vec<T> {
    fn try_map<F, U>(self, f: F) -> Result<Vec<U>, Error>
    where
        Self: Sized,
        F: (std::ops::Fn(T) -> Result<U, Error>) + Sized,
    {
        let mut output = Vec::with_capacity(self.len());
        for item in self {
            output.push((f)(item)?);
        }
        Ok(output)
    }
}

// Simple utility trait that makes it easier to apply `.try_map` to HashMap.
// This will convert one HashMap into another keeping the key the same, but
// different value type.
pub trait HashMapExt<K: std::cmp::Eq + std::hash::Hash, T> {
    fn try_map<F, U>(self, f: F) -> Result<HashMap<K, U>, Error>
    where
        Self: Sized,
        F: (std::ops::Fn(T) -> Result<U, Error>) + Sized;
}

impl<K: std::cmp::Eq + std::hash::Hash, T> HashMapExt<K, T> for HashMap<K, T> {
    fn try_map<F, U>(self, f: F) -> Result<HashMap<K, U>, Error>
    where
        Self: Sized,
        F: (std::ops::Fn(T) -> Result<U, Error>) + Sized,
    {
        let mut output = HashMap::with_capacity(self.len());
        for (k, v) in self {
            output.insert(k, (f)(v)?);
        }
        Ok(output)
    }
}

// Utility to encode our proto into GRPC stream format.
pub fn encode_stream_proto<T: Message>(proto: &T) -> Result<Bytes, Box<dyn std::error::Error>> {
    let mut buf = BytesMut::new();
    // See below comment on spec.
    use std::mem::size_of;
    const PREFIX_BYTES: usize = size_of::<u8>() + size_of::<u32>();
    for _ in 0..PREFIX_BYTES {
        // Advance our buffer first.
        // We will backfill it once we know the size of the message.
        buf.put_u8(0);
    }
    proto.encode(&mut buf)?;
    let len = buf.len() - PREFIX_BYTES;
    {
        let mut buf = &mut buf[0..PREFIX_BYTES];
        // See: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#:~:text=Compressed-Flag
        // for more details on spec.
        // Compressed-Flag -> 0 / 1 # encoded as 1 byte unsigned integer.
        buf.put_u8(0);
        // Message-Length -> {length of Message} # encoded as 4 byte unsigned integer (big endian).
        buf.put_u32(u32::try_from(len).expect("Could not convert usize to u32"));
        // Message -> *{binary octet}.
    }

    Ok(buf.freeze())
}
