use crate::common::regex_util::regex_utils::{get_prefix_matcher, get_suffix_matcher, simplify};
use regex::Error as RegexError;
use crate::common::regex_util::match_handlers::StringMatchHandler;

/// PromRegex implements an optimized string matching for Prometheus-like regex.
///
/// The following regexes are optimized:
///
/// - plain string such as "foobar"
/// - alternate strings such as "foo|bar|baz"
/// - prefix match such as "foo.*" or "foo.+"
/// - substring match such as ".*foo.*" or ".+bar.+"
///
/// The rest of regexps are also optimized by returning cached match results for the same input strings.
pub struct PromRegex {
    /// prefix contains literal prefix for regex.
    /// For example, prefix="foo" for regex="foo(a|b)"
    prefix: String,
    prefix_matcher: StringMatchHandler,
    suffix_matcher: StringMatchHandler,
}

impl PromRegex {
    pub fn new(expr: &str) -> Result<PromRegex, RegexError> {
        let (prefix, suffix) = simplify(expr)?;
        let pr = PromRegex {
            prefix: prefix.to_string(),
            prefix_matcher: get_prefix_matcher(&prefix),
            suffix_matcher: get_suffix_matcher(&suffix)?,
        };
        Ok(pr)
    }

    /// match_string returns true if s matches pr.
    ///
    /// The pr is automatically anchored to the beginning and to the end
    /// of the matching string with '^' and '$'.
    pub fn match_string(&self, s: &str) -> bool {
        let good = self.prefix_matcher.matches(s);
        if !good {
            return false
        }
        let suffix = s.strip_prefix(&self.prefix);
        if suffix.is_none() {
            // Fast path - s has another prefix than pr.
            return false;
        }
        let s = suffix.unwrap();
        self.suffix_matcher.matches(s)
    }
}