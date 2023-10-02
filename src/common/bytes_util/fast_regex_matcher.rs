use std::fmt::{Debug, Display};
use std::sync::Mutex;
use std::time::Duration;
use lru_time_cache::LruCache;
use regex::Regex;

const DEFAULT_CACHE_SIZE: usize = 100;
const DEFAULT_CACHE_EXPIRE_DURATION: Duration = Duration::from_secs(5 * 60);

/// FastRegexMatcher implements fast matcher for strings.
///
/// It caches string match results and returns them back on the next calls
/// without calling the match_func, which may be expensive.
pub struct FastRegexMatcher {
    regex: Regex,
    cache: Mutex<LruCache<String,bool>>,
}

impl FastRegexMatcher {
    /// creates new matcher which applies match_func to strings passed to matches()
    ///
    /// match_func must return the same result for the same input.
    pub fn new(regex: Regex) -> Self {
        return Self{
            regex,
            cache: Mutex::new(LruCache::with_expiry_duration_and_capacity(DEFAULT_CACHE_EXPIRE_DURATION, DEFAULT_CACHE_SIZE)),
        }
    }

    // Match applies match_func to s and returns the result.
    pub fn matches(&self, s: &str) -> bool {
        let mut inner = self.cache.lock().unwrap();
        if let Some(e) = inner.get(s) {
            return *e
        }
        // Slow path - run match_func for s and store the result in the cache.
        let value = self.regex.is_match(s);
        inner.insert(s.to_string(), value);
        value
    }
}

impl Clone for FastRegexMatcher {
    fn clone(&self) -> Self {
        let cache = self.cache.lock().unwrap();
        Self {
            regex: self.regex.clone(),
            cache: Mutex::new(cache.clone()),
        }
    }
}

impl PartialEq for FastRegexMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.regex.as_str() == other.regex.as_str()
    }
}

impl Display for FastRegexMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "/{}/", self.regex)
    }
}

impl Debug for FastRegexMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "/{}/", self.regex)
    }
}