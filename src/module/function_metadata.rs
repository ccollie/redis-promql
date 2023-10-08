// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata
use metricsql_parser::common::Matchers;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use crate::common::{parse_series_selector, parse_timestamp};
use crate::common::types::Timestamp;

const CMD_ARG_START: &str = "START";
const CMD_ARG_END: &str = "END";
const CMD_ARG_MATCH: &str = "MATCH";
const CMD_ARG_LABEL: &str = "LABEL";


pub(super) struct MetadataFunctionArgs {
    pub label_name: Option<String>,
    pub(crate) start: Option<Timestamp>,
    pub(crate) end: Option<Timestamp>,
    pub(crate) matchers: Vec<Matchers>,
}

pub(crate) fn parse_metadata_command_args(args: Vec<RedisString>, require_matchers: bool, need_label: bool) -> Result<MetadataFunctionArgs, RedisError> {
    let mut args = args.into_iter().skip(1);
    let mut start = None;
    let mut end = None;
    let mut label_name = None;
    let mut matchers = Vec::with_capacity(4);

    if need_label {
        label_name = Some(args.next_str()?.to_string());
    }

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_START) => {
                let next = args.next_str()?;
                if let Ok(ts) = parse_timestamp(&next) {
                    start = Some(ts);
                } else {
                    return Err(RedisError::Str("ERR invalid START timestamp"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_END) => {
                let next = args.next_str()?;
                if let Ok(ts) = parse_timestamp(&next) {
                    end = Some(ts);
                } else {
                    return Err(RedisError::Str("ERR invalid END timestamp"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_MATCH) => {
                while let Ok(matcher) = args.next_str() {
                    if let Ok(selector) = parse_series_selector(&matcher) {
                        matchers.push(selector);
                    } else {
                        return Err(RedisError::Str("ERR invalid MATCH series selector"));
                    }
                }
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(RedisError::String(msg));
            }
        };
    }

    if start > end {
        return Err(RedisError::Str("ERR invalid range"));
    }

    if require_matchers && matchers.is_empty() {
        return Err(RedisError::Str("ERR at least 1 MATCH series selector required"));
    }

    if need_label && label_name.is_none() {
        return Err(RedisError::Str("ERR missing label name"));
    }

    Ok(MetadataFunctionArgs {
        label_name,
        start,
        end,
        matchers,
    })
}
