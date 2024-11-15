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

pub mod cas_server;
pub mod schedulers;
pub mod serde_utils;
pub mod stores;

use schemars::JsonSchema;
use serde::de::IntoDeserializer;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NamedConfig<Spec> {
    pub name: String,
    #[serde(flatten)]
    pub spec: Spec,
}

#[derive(Debug, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NamedRef<Spec> {
    Name(String),
    Spec(Box<NamedConfig<Spec>>),
}

impl<Spec> NamedRef<Spec> {
    pub fn new<T>(name: impl Into<String>, spec: T) -> Self
    where
        T: Into<Spec>,
    {
        Self::Spec(Box::new(NamedConfig::<Spec> {
            name: name.into(),
            spec: spec.into(),
        }))
    }
}

impl<Spec> Serialize for NamedRef<Spec>
where
    Spec: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            NamedRef::Name(name) => name.serialize(serializer),
            NamedRef::Spec(config) => config.serialize(serializer),
        }
    }
}

impl<Spec> From<NamedConfig<Spec>> for NamedRef<Spec> {
    fn from(config: NamedConfig<Spec>) -> Self {
        NamedRef::Spec(Box::new(config))
    }
}

impl<'de, Spec> Deserialize<'de> for NamedRef<Spec>
where
    Spec: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // First, try to deserialize as a string
        let value = serde_json::Value::deserialize(deserializer)?;

        match value {
            // If it's a string, convert to Name variant
            serde_json::Value::String(s) => Ok(NamedRef::<Spec>::Name(s)),

            // If it's an object, try to deserialize as NamedConfig<Spec>
            serde_json::Value::Object(_) => {
                let store_config = NamedConfig::<Spec>::deserialize(value.into_deserializer())
                    .map_err(serde::de::Error::custom)?;
                Ok(NamedRef::<Spec>::Spec(Box::new(store_config)))
            }

            // Otherwise, return an error
            _ => Err(serde::de::Error::custom(
                "Expected either a string or an object for StoreRef enum",
            )),
        }
    }
}

/// This macro (and the invocation below) Implements the "From" trait for all
/// variations of the "Spec". For instance,  This allows patterns like this:
///
/// ```txt
/// crate::impl_from_spec!(
///    SchedulerSpec,
///    (Simple, SimpleSpec),
/// )
/// ```
///
/// resolves to:
///
/// ```txt
/// impl From<Simple> for SchedulerSpec {
///     fn from(spec: SimpleSpec) -> Self {
///         SchedulerSpec::Simple(spec);
///     }
/// }
/// ```
///
#[macro_export]
macro_rules! impl_from_spec {
    ($target:ident, $(($variant:ident, $spec:ident)),* $(,)?) => {
        $(
            impl From<$spec> for $target {
                fn from(spec: $spec) -> Self {
                    $target::$variant(spec)
                }
            }
        )*
    }
}
