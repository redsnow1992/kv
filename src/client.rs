use anyhow::Result;
use kv::{start_client_with_config, ClientConfig, CommandRequest, ProstClientStream};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config: ClientConfig = toml::from_str(include_str!("../fixtures/client.conf"))?;

    let stream = start_client_with_config(&config).await?;
    let mut client = ProstClientStream::new(stream);

    // 生成一个 HSET 命令
    let cmd = CommandRequest::new_hset("table1", "hello", "world".to_string().into());

    // 发送 HSET 命令
    let data = client.execute(cmd).await?;
    info!("Got response {:?}", data);

    Ok(())
}
