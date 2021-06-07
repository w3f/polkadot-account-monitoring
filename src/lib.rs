use anyhow::Error;

mod database;

type Result<T> = std::result::Result<T, Error>;