extern crate lru_time_cache;
mod common;
mod error;
mod module;
mod index;
mod provider;
mod globals;

pub(crate) use globals::*;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
