use anyhow::Result;
use kv::{start_server_with_config, ServerConfig};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config: ServerConfig = toml::from_str(include_str!("../fixtures/server.conf"))?;

    start_server_with_config(&config).await?;

    Ok(())
}
