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

use nativelink_config::serde_utils::{
    convert_data_size_with_shellexpand, convert_duration_with_shellexpand,
    convert_numeric_with_shellexpand, convert_optional_numeric_with_shellexpand,
    convert_optional_string_with_shellexpand, convert_string_with_shellexpand,
    convert_vec_string_with_shellexpand,
};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct DurationEntity {
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    duration: usize,
}

#[derive(Deserialize, Debug)]
struct DataSizeEntity {
    #[serde(deserialize_with = "convert_data_size_with_shellexpand")]
    data_size: u128,
}

#[derive(Deserialize, Debug, PartialEq)]
struct OptionalNumericEntity {
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    value: Option<usize>,
}

#[derive(Deserialize, Debug)]
struct OptionalStringEntity {
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    value: Option<String>,
}
#[derive(Deserialize, Debug)]
struct NumericEntity {
    #[serde(deserialize_with = "convert_numeric_with_shellexpand")]
    value: i64,
}
#[derive(Deserialize, Debug)]
struct StringEntity {
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    value: String,
}

#[derive(Deserialize, Debug)]
struct VecStringEntity {
    #[serde(deserialize_with = "convert_vec_string_with_shellexpand")]
    values: Vec<String>,
}

mod duration_tests {
    use super::*;

    #[test]
    fn test_duration_parsing() {
        let examples = [
            // Basic duration tests
            (r#"{"duration": "1m 10s"}"#, 70),
            (r#"{"duration": 10}"#, 10),
            (r#"{"duration": "  1m 10s  "}"#, 70),
            // Complex duration formats
            (r#"{"duration": "1y3w4d5h6m7s"}"#, 33_735_967),
            (r#"{"duration": "0s"}"#, 0),
            (r#"{"duration": "1ns"}"#, 0), // Sub-second rounds to 0
            (r#"{"duration": "999h"}"#, 3_596_400),
            // Large numbers
            (r#"{"duration": 0}"#, 0),
            (r#"{"duration": 1000}"#, 1000),
            // u32::MAX
            (r#"{"duration": 4294967295}"#, 4_294_967_295),
        ];

        for (input, expected) in examples {
            let deserialized: DurationEntity = serde_json5::from_str(input).unwrap();
            assert_eq!(deserialized.duration, expected);
        }
    }

    #[test]
    fn test_duration_negative_rejected() {
        let example = r#"{"duration": -10}"#;
        let result: Result<DurationEntity, _> = serde_json5::from_str(example);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Negative duration is not allowed"));
    }

    #[test]
    fn test_duration_errors() {
        let examples = [
            (
                r#"{"duration": true}"#,
                "expected either a number of seconds as an integer, or a string with a duration format (e.g., \"1h2m3s\", \"30m\", \"1d\")",
            ),
            (
                r#"{"duration": "invalid"}"#,
                "expected number at 0",
            ),
            (
                r#"{"duration": "999999999999999999999s"}"#,
                "number is too large",
            ),
        ];

        for (input, expected_error) in examples {
            let error = serde_json5::from_str::<DurationEntity>(input)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected_error));
        }
    }

    #[test]
    fn test_duration_whitespace_handling() {
        let example = r#"{"duration": "  1m 10s  "}"#;
        let deserialized: DurationEntity = serde_json5::from_str(example).unwrap();
        assert_eq!(deserialized.duration, 70);
    }

    #[test]
    fn test_large_duration_numbers() {
        let examples = [
            // u32::MAX
            (r#"{"duration": 4294967295}"#, 4_294_967_295),
            // u64::MAX - this will fail to parse as usize on 64-bit systems
            // (r#"{"duration": 18446744073709551615}"#, 18_446_744_073_709_551_615),
        ];

        for (input, expected) in examples {
            let deserialized: DurationEntity = serde_json5::from_str(input).unwrap();
            assert_eq!(deserialized.duration, expected);
        }
    }
}

mod data_size_tests {
    use super::*;

    #[test]
    fn test_data_size_parsing() {
        let examples = [
            // Basic size tests
            (r#"{"data_size": "1KiB"}"#, 1024),
            (r#"{"data_size": "1MiB"}"#, 1_048_576),
            (r#"{"data_size": "1MB"}"#, 1_000_000),
            (r#"{"data_size": "1M"}"#, 1_000_000),
            (r#"{"data_size": "1Mi"}"#, 1_048_576),
            // Large sizes
            (r#"{"data_size": "9EiB"}"#, 10_376_293_541_461_622_784),
            (r#"{"data_size": 10}"#, 10),
            // Edge cases
            (r#"{"data_size": "1B"}"#, 1),
            (r#"{"data_size": "1.5GB"}"#, 1_500_000_000),
            (r#"{"data_size": "1.5GiB"}"#, 1_610_612_736),
            (r#"{"data_size": "0B"}"#, 0),
            // Whitespace handling
            (r#"{"data_size": "  1KiB  "}"#, 1024),
        ];

        for (input, expected) in examples {
            let deserialized: DataSizeEntity = serde_json5::from_str(input).unwrap();
            assert_eq!(deserialized.data_size, expected);
        }
    }

    #[test]
    fn test_data_size_negative_rejected() {
        let example = r#"{"data_size": -1024}"#;
        let result: Result<DataSizeEntity, _> = serde_json5::from_str(example);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Negative data size is not allowed"));
    }

    #[test]
    fn test_data_size_case_insensitivity() {
        let examples = [
            r#"{"data_size": "1kb"}"#,
            r#"{"data_size": "1KB"}"#,
            r#"{"data_size": "1Kb"}"#,
            r#"{"data_size": "1kB"}"#,
        ];

        for input in examples {
            let deserialized: DataSizeEntity = serde_json5::from_str(input).unwrap();
            assert_eq!(deserialized.data_size, 1000); // All should be 1 kilobyte
        }
    }

    #[test]
    fn test_data_size_errors() {
        let examples = [
            (
                r#"{"data_size": true}"#,
                "expected either a number of bytes as an integer, or a string with a data size format (e.g., \"1GB\", \"500MB\", \"1.5TB\")",
            ),
            (
                r#"{"data_size": "invalid"}"#,
                "the character 'i' is not a number",
            ),
            (
                r#"{"data_size": "999999999999999999999B"}"#,
                "the value 999999999999999999999 exceeds the valid range",
            ),
        ];

        for (input, expected_error) in examples {
            let error = serde_json5::from_str::<DataSizeEntity>(input)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected_error));
        }
    }
}

mod optional_values_tests {
    use super::*;

    #[test]
    fn test_optional_numeric_values() {
        let examples = [
            (r#"{"value": null}"#, None),
            (r#"{"value": 42}"#, Some(42)),
            (r"{}", None),
        ];

        for (input, expected) in examples {
            let deserialized: OptionalNumericEntity = serde_json5::from_str(input).unwrap();
            assert_eq!(deserialized.value, expected);
        }
    }

    #[test]
    fn test_optional_numeric_large_numbers() {
        // Test i64::MAX for optional numeric
        let input = r#"{"value": "9223372036854775807"}"#;
        let result: OptionalNumericEntity = serde_json5::from_str(input).unwrap();
        assert_eq!(result.value, Some(9_223_372_036_854_775_807));
    }

    #[test]
    fn test_optional_numeric_errors() {
        let examples = [
            (
                r#"{"value": {}}"#,
                "expected an optional integer or a plain number string",
            ),
            (
                r#"{"value": "not_a_number"}"#,
                "invalid digit found in string",
            ),
            (
                r#"{"value": "999999999999999999999"}"#,
                "number too large to fit in target type",
            ),
        ];

        for (input, expected_error) in examples {
            let error = serde_json5::from_str::<OptionalNumericEntity>(input)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected_error));
        }
    }

    #[test]
    fn test_optional_string_values() {
        let examples = [
            (r#"{"value": ""}"#, Some(String::new())),
            (r#"{"value": null}"#, None),
            (r"{}", None),
            (r#"{"value": "   "}"#, Some("   ".to_string())),
        ];

        for (input, expected) in examples {
            let deserialized: OptionalStringEntity = serde_json5::from_str(input).unwrap();
            assert_eq!(deserialized.value, expected);
        }
    }

    #[test]
    fn test_mixed_optional_values() {
        #[derive(Deserialize)]
        struct MixedOptionals {
            #[serde(
                default,
                deserialize_with = "convert_optional_numeric_with_shellexpand"
            )]
            number: Option<usize>,
            #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
            string: Option<String>,
        }

        let examples = [
            (
                r#"{"number": null, "string": "hello"}"#,
                None,
                Some("hello".to_string()),
            ),
            (r#"{"number": 42, "string": null}"#, Some(42), None),
            (r#"{"number": null, "string": null}"#, None, None),
            (r"{}", None, None),
            (
                r#"{"number": null, "string": ""}"#,
                None,
                Some(String::new()),
            ),
            (
                r#"{"number": null, "string": "   "}"#,
                None,
                Some("   ".to_string()),
            ),
        ];

        for (input, expected_number, expected_string) in examples {
            let deserialized: MixedOptionals = serde_json5::from_str(input).unwrap();
            assert_eq!(deserialized.number, expected_number);
            assert_eq!(deserialized.string, expected_string);
        }
    }
}

mod shellexpand_tests {
    use super::*;

    #[test]
    fn test_shellexpand_functionality() {
        std::env::set_var("TEST_DURATION", "5m");
        std::env::set_var("TEST_SIZE", "1GB");
        std::env::set_var("TEST_NUMBER", "42");
        std::env::set_var("TEST_VAR", "test_value");
        std::env::set_var("EMPTY_VAR", "");

        let duration_result =
            serde_json5::from_str::<DurationEntity>(r#"{"duration": "${TEST_DURATION}"}"#).unwrap();
        assert_eq!(duration_result.duration, 300);

        let size_result =
            serde_json5::from_str::<DataSizeEntity>(r#"{"data_size": "${TEST_SIZE}"}"#).unwrap();
        assert_eq!(size_result.data_size, 1_000_000_000);

        let numeric_result =
            serde_json5::from_str::<OptionalNumericEntity>(r#"{"value": "${TEST_NUMBER}"}"#)
                .unwrap();
        assert_eq!(numeric_result.value, Some(42));

        let string_result =
            serde_json5::from_str::<OptionalStringEntity>(r#"{"value": "${TEST_VAR}"}"#).unwrap();
        assert_eq!(string_result.value, Some("test_value".to_string()));

        let empty_string_result =
            serde_json5::from_str::<OptionalStringEntity>(r#"{"value": "${EMPTY_VAR}"}"#).unwrap();
        assert_eq!(empty_string_result.value, Some(String::new()));

        let undefined_result =
            serde_json5::from_str::<OptionalNumericEntity>(r#"{"value": "${UNDEFINED_VAR}"}"#);
        assert!(undefined_result
            .unwrap_err()
            .to_string()
            .contains("environment variable not found"));
    }
}
#[cfg(test)]
mod convert_numeric_with_shellexpand_tests {
    use std::env;

    use serde_test::{assert_de_tokens_error, Token};

    use super::*;

    #[test]
    fn test_numeric_parsing() {
        let json = r#"{"value": 42}"#;
        let deserialized: NumericEntity = serde_json5::from_str(json).unwrap();
        assert_eq!(deserialized.value, 42);

        let json_str = r#"{"value": "42"}"#;
        let deserialized_str: NumericEntity = serde_json5::from_str(json_str).unwrap();
        assert_eq!(deserialized_str.value, 42);
    }

    #[test]
    fn test_numeric_parsing_with_env_var() {
        env::set_var("TEST_NUMERIC", "42");
        let json_env = r#"{"value": "${TEST_NUMERIC}"}"#;
        let deserialized_env: NumericEntity = serde_json5::from_str(json_env).unwrap();
        assert_eq!(deserialized_env.value, 42);
    }

    #[test]
    fn test_numeric_parsing_with_large_values() {
        let large_value = "9223372036854775807"; // i64::MAX
        let json = format!(r#"{{"value": "{large_value}"}}"#);
        let deserialized: NumericEntity = serde_json5::from_str(&json).unwrap();
        assert_eq!(deserialized.value, 9_223_372_036_854_775_807);
    }

    #[test]
    fn test_numeric_invalid_value_error() {
        let invalid_json = r#"{"value": "not_a_number"}"#;
        let result = serde_json5::from_str::<NumericEntity>(invalid_json);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid digit found in string"));
    }

    #[test]
    fn test_numeric_invalid_env_var_error() {
        let invalid_json = r#"{"value": "${UNDEFINED_ENV_VAR}"}"#;
        let result = serde_json5::from_str::<NumericEntity>(invalid_json);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("environment variable not found"));
    }

    #[test]
    fn test_expectation_error_message() {
        let invalid_json = r#"{"value": true}"#;
        let result = serde_json5::from_str::<NumericEntity>(invalid_json);
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(error_message.contains("an integer or a plain number string"));
    }

    #[test]
    fn test_visit_u64_within_i64_range() {
        let json = r#"{"value": 9223372036854775807}"#;
        let deserialized: NumericEntity = serde_json5::from_str(json).unwrap();
        assert_eq!(deserialized.value, 9_223_372_036_854_775_807);
    }

    #[test]
    fn test_visit_u64_exceeds_i64_range() {
        assert_de_tokens_error::<NumericEntity>(
            &[
                Token::Map { len: Some(1) },
                Token::Str("value"),
                Token::U64(9_223_372_036_854_775_808),
                Token::MapEnd,
            ],
            "out of range integral type conversion attempted",
        );
    }
}
#[cfg(test)]
mod shellexpand_string_tests {
    use std::env;

    use super::*;

    #[test]
    fn test_convert_string_with_shellexpand() {
        env::set_var("TEST_STRING", "expanded_value");

        let json = r#"{"value": "${TEST_STRING}"}"#;

        let deserialized: StringEntity = serde_json5::from_str(json).unwrap();

        assert_eq!(deserialized.value, "expanded_value");
    }

    #[test]
    fn test_convert_vec_string_with_shellexpand() {
        env::set_var("TEST_VAR1", "value1");
        env::set_var("TEST_VAR2", "value2");

        let json = r#"{"values": ["${TEST_VAR1}", "static_value", "${TEST_VAR2}"]}"#;
        let deserialized: VecStringEntity = serde_json5::from_str(json).unwrap();

        assert_eq!(
            deserialized.values,
            vec!["value1", "static_value", "value2"]
        );
    }
}
#[cfg(test)]
mod convert_optional_numeric_with_shellexpand_tests {

    use serde_test::{assert_de_tokens_error, Token};

    use super::*;

    #[test]
    fn test_visit_unit_returns_none() {
        serde_test::assert_de_tokens(
            &OptionalNumericEntity { value: None },
            &[
                Token::Map { len: Some(1) },
                Token::Str("value"),
                Token::Unit,
                Token::MapEnd,
            ],
        );
    }

    #[test]
    fn test_visit_u64_within_i64_range() {
        let json = r#"{"value": 9223372036854775807}"#;
        let deserialized: OptionalNumericEntity = serde_json5::from_str(json).unwrap();
        assert_eq!(deserialized.value, Some(9_223_372_036_854_775_807));
    }

    #[test]
    fn test_visit_u64_exceeds_i64_range() {
        assert_de_tokens_error::<OptionalNumericEntity>(
            &[
                Token::Map { len: Some(1) },
                Token::Str("value"),
                Token::U64(9_223_372_036_854_775_808),
                Token::MapEnd,
            ],
            "out of range integral type conversion attempted",
        );
    }

    #[test]
    fn test_visit_some_valid_value() {
        let json = r#"{"value": "42"}"#;
        let deserialized: OptionalNumericEntity = serde_json5::from_str(json).unwrap();
        assert_eq!(deserialized.value, Some(42));
    }

    #[test]
    fn test_visit_str_empty_string_error() {
        let json = r#"{"value": ""}"#;
        let result = serde_json5::from_str::<OptionalNumericEntity>(json);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("empty string is not a valid number"));
    }

    #[test]
    fn test_visit_str_whitespace_only() {
        let json = r#"{"value": "   "}"#;
        let deserialized: OptionalNumericEntity = serde_json5::from_str(json).unwrap();
        assert_eq!(deserialized.value, None);
    }
}
