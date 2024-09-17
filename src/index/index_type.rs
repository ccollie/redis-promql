use std::fmt::Display;
use std::ops::Deref;
use blart::AsBytes;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey(Box<[u8]>);

const SENTINEL: u8 = 0;

impl IndexKey {
    pub fn new(key: &str) -> Self {
        Self::from(key)
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0[..self.0.len() - 1]).unwrap()
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
        let mut key = utf8.as_bytes().into_boxed_slice();
        key.push(SENTINEL);
        IndexKey(key)
    }
}

impl From<Vec<u8>> for IndexKey {
    fn from(key: Vec<u8>) -> Self {
        Self::from(key.as_bytes())
    }
}

impl From<&str> for IndexKey {
    fn from(key: &str) -> Self {
        key.as_bytes().into()
    }
}
