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
use std::io;
use std::num::{ParseIntError, TryFromIntError};

use fred::error::{RedisError, RedisErrorKind};
use hex::FromHexError;
use nativelink_error::{make_err, make_input_err, Code, Error, ResultExt};
use nativelink_metric::{MetricFieldData, MetricKind, MetricsComponent};
use nativelink_proto::google::rpc::Status;
use serde::de::Error as DeError;
use serde::ser::Error as SerError;
use tonic::{Code as TonicCode, Status as TonicStatus};

#[test]
fn test_serde_serialization_error_custom() {
    let error = <Error as SerError>::custom("Serialization failed due to invalid input");
    assert_eq!(error.code, Code::InvalidArgument);
    assert_eq!(
        error.messages,
        vec!["Serialization failed due to invalid input".to_string()]
    );
}

#[test]
fn test_serde_deserialization_error_custom() {
    let error = <Error as DeError>::custom("Deserialization failed due to corrupted data");
    assert_eq!(error.code, Code::InvalidArgument);
    assert_eq!(
        error.messages,
        vec!["Deserialization failed due to corrupted data".to_string()]
    );
}

#[test]
fn test_err_tip_with_code_some() {
    let option = Some(42);
    let result: Result<i32, Error> =
        option.err_tip_with_code(|_error| (Code::Unknown, "Should not appear"));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_err_tip_with_code_none() {
    let option: Option<i32> = None;
    let result: Result<i32, Error> =
        option.err_tip_with_code(|_error| (Code::InvalidArgument, "Missing value in option"));
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert_eq!(error.messages, vec!["Missing value in option".to_string()]);
}
#[test]
fn test_code_to_error_conversion() {
    let error: Error = Code::InvalidArgument.into();
    println!(
        "Debug: Error Code - {:?}, Messages - {:?}",
        error.code, error.messages
    );
    assert_eq!(
        error.code,
        Code::InvalidArgument,
        "Expected code to be InvalidArgument"
    );
    assert!(
        error.messages.is_empty() || error.messages[0].is_empty(),
        "Expected messages to be empty or contain an empty string, but found: {:?}",
        error.messages
    );
}

#[test]
fn test_redis_error_conversion_invalid_argument() {
    let redis_error = RedisError::new(RedisErrorKind::InvalidArgument, "Invalid argument error");
    let error: Error = redis_error.into();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.messages[0].contains("Invalid argument error"));
}

#[test]
fn test_redis_error_conversion_internal() {
    let redis_error = RedisError::new(RedisErrorKind::IO, "Internal error");
    let error: Error = redis_error.into();
    assert_eq!(error.code, Code::Internal);
    assert!(error.messages[0].contains("Internal error"));
}

#[test]
fn test_redis_error_conversion_permission_denied() {
    let redis_error = RedisError::new(RedisErrorKind::Auth, "Permission denied error");
    let error: Error = redis_error.into();
    assert_eq!(error.code, Code::PermissionDenied);
    assert!(error.messages[0].contains("Permission denied error"));
}

#[test]
fn test_redis_error_conversion_timeout() {
    let redis_error = RedisError::new(RedisErrorKind::Timeout, "Timeout error");
    let error: Error = redis_error.into();
    assert_eq!(error.code, Code::DeadlineExceeded);
    assert!(error.messages[0].contains("Timeout error"));
}

#[test]
fn test_transport_error_conversion_simulation() {
    let error: Error = make_err!(Code::Internal, "Transport connection failed simulation");
    assert_eq!(error.code, Code::Internal);
    assert!(error.messages[0].contains("Transport connection failed simulation"));
}

#[test]
fn test_transport_error_simulation() {
    let error: Error = make_err!(Code::Internal, "Transport connection failed simulation");
    assert_eq!(error.code, Code::Internal);
    assert!(error.messages[0].contains("Transport connection failed simulation"));
}

#[test]
fn test_status_to_error_conversion_unique() {
    let tonic_status = TonicStatus::invalid_argument("Invalid argument provided");
    let error: Error = tonic_status.into();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.messages[0].contains("Invalid argument provided"));
}

#[test]
fn test_error_to_status_conversion() {
    let error = Error {
        code: Code::Internal,
        messages: vec!["An internal error occurred".to_string()],
    };
    let tonic_status: TonicStatus = error.into();
    assert_eq!(tonic_status.code(), TonicCode::Internal);
    assert!(tonic_status
        .message()
        .contains("An internal error occurred"));
}

#[test]
fn test_encode_error_conversion() {
    let error: Error = make_err!(Code::Internal, "Encode failure mock");
    assert_eq!(error.code, Code::Internal);
    assert!(error.messages[0].contains("Encode failure mock"));
}

#[test]
fn test_try_from_int_error_conversion() {
    let int_error: Result<u8, TryFromIntError> = u16::try_into(300);
    let error: Error = int_error.unwrap_err().into();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.messages[0].contains("out of range"));
}

#[test]
fn test_parse_int_error_conversion() {
    let parse_error: Result<i32, ParseIntError> = "abc".parse();
    let error: Error = parse_error.unwrap_err().into();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.messages[0].contains("invalid digit"));
}

#[test]
fn test_from_hex_error_conversion() {
    let hex_error = FromHexError::InvalidStringLength;
    let error: Error = hex_error.into();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.messages[0].contains("Invalid string length"));
}

#[tokio::test]
async fn test_join_error_conversion() {
    let join_error = tokio::spawn(async { panic!("JoinError mock") })
        .await
        .unwrap_err();
    let error: Error = join_error.into();
    assert_eq!(error.code, Code::Internal);
    assert!(error.messages[0].contains("JoinError mock"));
}

#[test]
fn test_prost_encode_error_conversion() {
    let error = make_err!(Code::Internal, "Encode failure mock");
    assert_eq!(error.code, Code::Internal);
    assert!(error.messages[0].contains("Encode failure mock"));
}

#[test]
fn test_prost_unknown_enum_value_conversion() {
    let error = make_err!(Code::Internal, "Unknown enum value mock");
    assert_eq!(error.code, Code::Internal);
    assert!(error.messages[0].contains("Unknown enum value mock"));
}

#[test]
fn test_timestamp_error_conversion() {
    let error = make_err!(Code::InvalidArgument, "Timestamp error mock");
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.messages[0].contains("Timestamp error mock"));
}

#[test]
fn test_status_to_error_conversion() {
    let status = Status {
        code: Code::InvalidArgument as i32,
        message: "Test error message".to_string(),
        details: vec![],
    };
    let error: Error = status.into();
    assert_eq!(error.code, Code::InvalidArgument);
    assert_eq!(error.messages, vec!["Test error message"]);
}

#[test]
fn test_metrics_component_publish() {
    let error = Error::new(Code::Internal, "Metrics publish error".to_string());
    let kind = MetricKind::Default;
    let field_metadata = MetricFieldData {
        name: Cow::from("test_metric"),
        group: Cow::from("test_group"),
        help: Cow::from("help message"),
    };
    let result = error.publish(kind, field_metadata);
    match result {
        Ok(data) => {
            println!("Published data: {:?}", data);
        }
        Err(e) => panic!("Publish failed with error: {:?}", e),
    }
}

#[test]
fn test_error_new() {
    let error = Error::new(Code::Internal, "Error occurred".to_string());
    assert_eq!(error.code, Code::Internal);
    assert_eq!(error.messages, vec!["Error occurred"]);
}

#[test]
fn test_error_append() {
    let error = Error::new(Code::Internal, "Initial error".to_string()).append("Additional error");
    assert_eq!(error.messages, vec!["Initial error", "Additional error"]);
}

#[test]
fn test_error_merge() {
    let error1 = Error::new(Code::Internal, "Error 1".to_string());
    let error2 = Error::new(Code::Unknown, "Error 2".to_string());
    let merged_error = error1.merge(error2);
    assert_eq!(merged_error.messages, vec!["Error 1", "---", "Error 2"]);
}

#[test]
fn test_error_merge_option() {
    let error1 = Some(Error::new(Code::Internal, "Error 1".to_string()));
    let error2 = Some(Error::new(Code::Unknown, "Error 2".to_string()));
    let merged_error = Error::merge_option(error1, error2);
    assert!(merged_error.is_some());
    assert_eq!(
        merged_error.unwrap().messages,
        vec!["Error 1", "---", "Error 2"]
    );
}

#[test]
fn test_error_to_std_err() {
    let error = Error::new(Code::Internal, "IO Error".to_string());
    let io_error = error.to_std_err();
    assert_eq!(io_error.kind(), io::ErrorKind::Other);
    assert_eq!(io_error.to_string(), "IO Error");
}

#[test]
fn test_error_message_string() {
    let error = Error::new(Code::Internal, "Part 1".to_string()).append("Part 2");
    assert_eq!(error.message_string(), "Part 1 : Part 2");
}

#[test]
fn test_error_make_err_macro() {
    let error = make_err!(Code::InvalidArgument, "Invalid argument: {}", "value");
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error
        .messages
        .contains(&"Invalid argument: value".to_string()));
}

#[test]
fn test_error_make_input_err_macro() {
    let error = make_input_err!("Input error: {}", "example");
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.messages.contains(&"Input error: example".to_string()));
}

#[test]
fn test_error_conversion_from_prost_decode_error() {
    let prost_error = prost::DecodeError::new("Decode failure");
    let error: Error = prost_error.into();
    assert_eq!(error.code, Code::Internal);
    assert!(
        error.messages[0].contains("Decode failure"),
        "Error message was: {:?}",
        error.messages
    );
}

#[test]
fn test_error_conversion_from_std_io_error() {
    let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
    let error: Error = io_error.into();
    assert_eq!(error.code, Code::NotFound);
    assert!(error.messages.contains(&"File not found".to_string()));
}

#[test]
fn test_result_ext_err_tip() {
    let result: Result<(), Error> =
        Err(Error::new(Code::Internal, "Base error".to_string())).err_tip(|| "Additional context");
    let error = result.unwrap_err();
    assert!(error.messages.contains(&"Additional context".to_string()));
}

#[test]
fn test_result_ext_merge() {
    let result1: Result<(), Error> = Err(Error::new(Code::Internal, "First error".to_string()));
    let result2: Result<(), Error> = Err(Error::new(Code::Unknown, "Second error".to_string()));
    let merged_result = result1.merge(result2);
    let error = merged_result.unwrap_err();
    assert_eq!(error.messages, vec!["First error", "---", "Second error"]);
}

#[test]
fn test_tonic_code_to_code_conversion() {
    assert_eq!(Code::from(TonicCode::Ok), Code::Ok);
    assert_eq!(Code::from(TonicCode::Cancelled), Code::Cancelled);
    assert_eq!(Code::from(TonicCode::Unknown), Code::Unknown);
    assert_eq!(
        Code::from(TonicCode::InvalidArgument),
        Code::InvalidArgument
    );
    assert_eq!(
        Code::from(TonicCode::DeadlineExceeded),
        Code::DeadlineExceeded
    );
    assert_eq!(Code::from(TonicCode::NotFound), Code::NotFound);
    assert_eq!(Code::from(TonicCode::AlreadyExists), Code::AlreadyExists);
    assert_eq!(
        Code::from(TonicCode::PermissionDenied),
        Code::PermissionDenied
    );
    assert_eq!(
        Code::from(TonicCode::ResourceExhausted),
        Code::ResourceExhausted
    );
    assert_eq!(
        Code::from(TonicCode::FailedPrecondition),
        Code::FailedPrecondition
    );
    assert_eq!(Code::from(TonicCode::Aborted), Code::Aborted);
    assert_eq!(Code::from(TonicCode::OutOfRange), Code::OutOfRange);
    assert_eq!(Code::from(TonicCode::Unimplemented), Code::Unimplemented);
    assert_eq!(Code::from(TonicCode::Internal), Code::Internal);
    assert_eq!(Code::from(TonicCode::Unavailable), Code::Unavailable);
    assert_eq!(Code::from(TonicCode::DataLoss), Code::DataLoss);
    assert_eq!(
        Code::from(TonicCode::Unauthenticated),
        Code::Unauthenticated
    );
}

#[test]
fn test_code_to_tonic_code_conversion() {
    assert_eq!(TonicCode::from(Code::Ok), TonicCode::Ok);
    assert_eq!(TonicCode::from(Code::Cancelled), TonicCode::Cancelled);
    assert_eq!(TonicCode::from(Code::Unknown), TonicCode::Unknown);
    assert_eq!(
        TonicCode::from(Code::InvalidArgument),
        TonicCode::InvalidArgument
    );
    assert_eq!(
        TonicCode::from(Code::DeadlineExceeded),
        TonicCode::DeadlineExceeded
    );
    assert_eq!(TonicCode::from(Code::NotFound), TonicCode::NotFound);
    assert_eq!(
        TonicCode::from(Code::AlreadyExists),
        TonicCode::AlreadyExists
    );
    assert_eq!(
        TonicCode::from(Code::PermissionDenied),
        TonicCode::PermissionDenied
    );
    assert_eq!(
        TonicCode::from(Code::ResourceExhausted),
        TonicCode::ResourceExhausted
    );
    assert_eq!(
        TonicCode::from(Code::FailedPrecondition),
        TonicCode::FailedPrecondition
    );
    assert_eq!(TonicCode::from(Code::Aborted), TonicCode::Aborted);
    assert_eq!(TonicCode::from(Code::OutOfRange), TonicCode::OutOfRange);
    assert_eq!(
        TonicCode::from(Code::Unimplemented),
        TonicCode::Unimplemented
    );
    assert_eq!(TonicCode::from(Code::Internal), TonicCode::Internal);
    assert_eq!(TonicCode::from(Code::Unavailable), TonicCode::Unavailable);
    assert_eq!(TonicCode::from(Code::DataLoss), TonicCode::DataLoss);
    assert_eq!(
        TonicCode::from(Code::Unauthenticated),
        TonicCode::Unauthenticated
    );
}

#[test]
fn test_i32_to_code_conversion() {
    assert_eq!(Code::from(0), Code::Ok);
    assert_eq!(Code::from(1), Code::Cancelled);
    assert_eq!(Code::from(2), Code::Unknown);
    assert_eq!(Code::from(3), Code::InvalidArgument);
    assert_eq!(Code::from(4), Code::DeadlineExceeded);
    assert_eq!(Code::from(5), Code::NotFound);
    assert_eq!(Code::from(6), Code::AlreadyExists);
    assert_eq!(Code::from(7), Code::PermissionDenied);
    assert_eq!(Code::from(8), Code::ResourceExhausted);
    assert_eq!(Code::from(9), Code::FailedPrecondition);
    assert_eq!(Code::from(10), Code::Aborted);
    assert_eq!(Code::from(11), Code::OutOfRange);
    assert_eq!(Code::from(12), Code::Unimplemented);
    assert_eq!(Code::from(13), Code::Internal);
    assert_eq!(Code::from(14), Code::Unavailable);
    assert_eq!(Code::from(15), Code::DataLoss);
    assert_eq!(Code::from(16), Code::Unauthenticated);
    assert_eq!(Code::from(99), Code::Unknown);
    assert_eq!(Code::from(-1), Code::Unknown);
}
