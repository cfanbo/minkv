use log::error;
use minkv::cli;

#[tokio::main]
async fn main() {
    match cli::cli::parse().await {
        Ok(_) => {}
        Err(e) => {
            error!("{:?}", e);
            eprintln!("{:?}", e)
        }
    }
}
