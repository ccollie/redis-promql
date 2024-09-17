use std::fmt::Write;
use std::borrow::Borrow;
use std::fmt::Display;
use std::ops::Deref;
use blart::AsBytes;
use metricsql_runtime::prelude::METRIC_NAME_LABEL;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey(Box<[u8]>);

const SENTINEL: u8 = 0;

impl IndexKey {
    pub fn new(key: &str) -> Self {
        Self::from(key)
    }

    pub fn for_metric_name(metric_name: &str) -> Self {
        let value = get_key_for_metric_name(metric_name);
        Self::new(&value)
    }

    pub fn for_label_value(label_name: &str, value: &str) -> Self {
        let value = get_key_for_label_value(label_name, value);
        Self::new(&value)
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0[..self.0.len() - 1]).unwrap()
    }

    pub fn split(&self) -> Option<(&str, &str)> {
        let key = self.as_str();
        if let Some(index) = key.find('=') {
            Some((&key[..index], &key[index + 1..]))
        } else {
            None
        }
    }

    pub(super) fn sub_string(&self, start: usize) -> &str {
        std::str::from_utf8(&self.0[start..self.0.len() - 1]).unwrap()
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0.into_vec()
    }

    pub fn len(&self) -> usize {
        self.0.len() - 1
    }
}

impl Display for IndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Deref for IndexKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsBytes for IndexKey {
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for IndexKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<&[u8]> for IndexKey {
    fn from(key: &[u8]) -> Self {
        let utf8 = String::from_utf8_lossy(key);
        let mut v = utf8.as_bytes().to_vec();
        v.push(SENTINEL);
        IndexKey(v.into_boxed_slice())
    }
}

impl From<Vec<u8>> for IndexKey {
    fn from(key: Vec<u8>) -> Self {
        Self::from(key.as_bytes())
    }
}

impl From<&str> for IndexKey {
    fn from(key: &str) -> Self {
        let mut key = key.as_bytes().to_vec();
        key.push(SENTINEL);
        IndexKey(key.into_boxed_slice())
    }
}

impl Borrow<[u8]> for IndexKey {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

pub fn format_key_for_label_prefix(dest: &mut String, label_name: &str) {
    dest.clear();
    // write! does not panic, unwrap is safe
    write!(dest, "{label_name}=").unwrap();
}

pub fn format_key_for_label_value(dest: &mut String, label_name: &str, value: &str) {
    dest.clear();
    // according to https://github.com/rust-lang/rust/blob/1.47.0/library/alloc/src/string.rs#L2414-L2427
    // write! will not return an Err, so the unwrap is safe
    write!(dest, "{label_name}={value}{SENTINEL}").unwrap();
}

pub fn format_key_for_metric_name(dest: &mut String, metric_name: &str) {
    format_key_for_label_value(dest, METRIC_NAME_LABEL, metric_name);
}

pub fn get_key_for_label_prefix(label_name: &str) -> String {
    let mut value = String::with_capacity(label_name.len() + 1);
    format_key_for_label_prefix(&mut value, label_name);
    value
}

pub fn get_key_for_label_value(label_name: &str, value: &str) -> String {
    let mut res = String::with_capacity(label_name.len() + value.len() + 1);
    format_key_for_label_value(&mut res, label_name, value);
    res
}

pub fn get_key_for_metric_name(metric_name: &str) -> String {
    get_key_for_label_value(METRIC_NAME_LABEL, metric_name)
}