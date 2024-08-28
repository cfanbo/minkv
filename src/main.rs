use log::error;
use minkv::cli;

#[tokio::main]
async fn main() {
    // tokio-console
    #[cfg(debug_assertions)]
    {
        console_subscriber::init();
    }

    match cli::cli::parse().await {
        Ok(_) => {}
        Err(e) => {
            error!("{:?}", e);
            eprintln!("{:?}", e)
        }
    }
}
