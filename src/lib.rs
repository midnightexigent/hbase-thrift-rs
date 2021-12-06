pub mod client;
pub mod error;
pub mod scan;

#[allow(clippy::all, dead_code)]
pub mod hbase;

pub use error::Error;

type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
