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

use std::borrow::Cow;
use std::fmt::Debug;

use nativelink_metric::MetricKind;
use serde::Serialize;
use tracing::field::{Field, Visit};

use crate::metrics_collection::{CollectedMetricPrimitive, CollectedMetricPrimitiveValue};

/// The type of the collected primitive metric.
#[derive(Default, Debug, Serialize)]
pub enum CollectionKind {
    #[default]
    Counter = 0,
    String = 1,
}

impl From<MetricKind> for CollectionKind {
    fn from(kind: MetricKind) -> Self {
        match kind {
            MetricKind::Counter => CollectionKind::Counter,
            MetricKind::Default | MetricKind::String | MetricKind::Component => {
                CollectionKind::String
            }
        }
    }
}

#[derive(Debug)]
pub enum ValueWithPrimitiveType {
    String(String),
    U64(u64),
}

impl Default for ValueWithPrimitiveType {
    fn default() -> Self {
        ValueWithPrimitiveType::U64(0)
    }
}
#[derive(Default, Debug)]
pub struct MetricDataVisitor {
    pub name: String,
    pub value: ValueWithPrimitiveType,
    pub help: String,
    pub value_type: Option<CollectionKind>,
}

impl From<MetricDataVisitor> for CollectedMetricPrimitive {
    fn from(visitor: MetricDataVisitor) -> Self {
        let (value, derived_type) = match visitor.value {
            ValueWithPrimitiveType::String(s) => (
                CollectedMetricPrimitiveValue::String(Cow::Owned(s)),
                CollectionKind::String,
            ),
            ValueWithPrimitiveType::U64(u) => (
                CollectedMetricPrimitiveValue::Counter(u),
                CollectionKind::Counter,
            ),
        };
        CollectedMetricPrimitive {
            value: Some(value),
            help: visitor.help,
            value_type: visitor.value_type.unwrap_or(derived_type),
        }
    }
}

impl MetricDataVisitor {
    pub fn record_test_value(&mut self, field_name: &str, value: &str) {
        match field_name {
            "name" => self.name = value.to_string(),
            "__help" => self.help = value.to_string(),
            "__value" => {
                if let Ok(parsed) = value.parse::<u64>() {
                    self.value = ValueWithPrimitiveType::U64(parsed);
                } else {
                    self.value = ValueWithPrimitiveType::String(value.to_string());
                }
            }
            "test_key" => {}
            _ => panic!("Unknown field: {field_name}"),
        }
    }
}

impl Visit for MetricDataVisitor {
    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {}

    fn record_f64(&mut self, field: &Field, value: f64) {
        if field.name() == "__value" {
            self.value = ValueWithPrimitiveType::String(value.to_string());
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "__value" {
            self.value = match u64::try_from(value) {
                Ok(v) => ValueWithPrimitiveType::U64(v),
                Err(_) => ValueWithPrimitiveType::String(value.to_string()),
            };
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "__value" => self.value = ValueWithPrimitiveType::U64(value),
            "__type" => self.value_type = Some(CollectionKind::from(MetricKind::from(value))),
            "__help" => self.help = value.to_string(),
            "__name" => self.name = value.to_string(),
            "test_key" => {}
            _ => panic!("UNKNOWN FIELD {field}"),
        }
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        if field.name() == "__value" {
            self.value = match u64::try_from(value) {
                Ok(v) => ValueWithPrimitiveType::U64(v),
                Err(_) => ValueWithPrimitiveType::String(value.to_string()),
            };
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        if field.name() == "__value" {
            self.value = match u64::try_from(value) {
                Ok(v) => ValueWithPrimitiveType::U64(v),
                Err(_) => ValueWithPrimitiveType::String(value.to_string()),
            };
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "__value" {
            self.value = ValueWithPrimitiveType::U64(u64::from(value));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "__value" => self.value = ValueWithPrimitiveType::String(value.to_string()),
            "__help" => self.help = value.to_string(),
            "__name" => self.name = value.to_string(),
            "test_key" => {}
            _ => panic!("UNKNOWN FIELD {field}"),
        }
    }

    fn record_error(&mut self, _field: &Field, _value: &(dyn std::error::Error + 'static)) {}
}

pub struct SpanFields {
    pub name: Cow<'static, str>,
}

impl Visit for SpanFields {
    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {}

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "__name" {
            self.name = Cow::Owned(value.to_string());
        }
    }
}
