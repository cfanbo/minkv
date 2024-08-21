use log::debug;
use minkv::cli;
use serde::{Deserialize, Serialize};

// 派生 Serialize 和 Deserialize
#[derive(Serialize, Deserialize)]
struct Record {
    id: u32,
    name: String,
    age: u8,
}

#[tokio::main]
async fn main() {
    match cli::cli::parse().await {
        Ok(_) => {}
        Err(e) => {
            debug!("{:?}", e);
            println!("{:?}", e)
        }
    }
}
