use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use lru_time_cache::LruCache;

#[derive(Clone, Default, Debug)]
pub(crate) struct PrefixSuffix {
    pub(crate) prefix: String,
    pub(crate) suffix: String
}
impl PrefixSuffix {
    pub fn new(prefix: String, suffix: String) -> Self {
        Self {
            prefix,
            suffix
        }
    }

    pub fn size_bytes(&self) -> usize {
        &self.prefix.len() + &self.suffix.len() + std::mem::size_of::<Self>()
    }
}

pub struct PrefixCache {
    requests: AtomicU64,
    misses: AtomicU64,
    max_size_bytes: usize,
    inner: Mutex<Inner>,
}

struct Inner {
    size_bytes: usize,
    cache: LruCache<String, Arc<PrefixSuffix>>,
}

impl PrefixCache {
    pub fn new(max_size_bytes: usize) -> Self {
        let inner = Inner {
            size_bytes: 0,
            cache: LruCache::with_capacity(100), // todo!!!!!!
        };
        Self {
            requests: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inner: Mutex::new(inner),
            max_size_bytes,
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<Arc<PrefixSuffix>> {
        let mut inner = self.inner.lock().unwrap();
        let item = inner.cache.get(key);
        if item.is_some() {
            self.requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        item.cloned()
    }

    pub(crate) fn put(&self, key: &str, value: Arc<PrefixSuffix>) {
        let mut inner = self.inner.lock().unwrap();
        let size = value.size_bytes();
        if inner.size_bytes + size > self.max_size_bytes {
            // todo
        }
        inner.size_bytes += size;
        inner.cache.insert(key.to_string(), value);
    }

    /// returns the number of cached regexps for tag filters.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().cache.is_empty()
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().cache.clear();
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub fn size_bytes(&self) -> usize {
        self.inner.lock().unwrap().size_bytes
    }

    pub fn max_size_bytes(&self) -> usize {
        self.max_size_bytes
    }
}