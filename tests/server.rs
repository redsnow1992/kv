use std::time::Duration;

use anyhow::Result;
use kv::{
    start_client_with_config, start_server_with_config, ClientConfig, CommandRequest,
    ProstClientStream, ServerConfig, StorageConfig,
};
use tokio::time;

#[tokio::test]
async fn server_client_full_tests() -> Result<()> {
    let addr = "127.0.0.1:10086";

    let mut config: ServerConfig = toml::from_str(include_str!("../fixtures/server.conf"))?;
    config.general.addr = addr.into();
    config.storage = StorageConfig::MemTable;

    // 启动服务器
    tokio::spawn(async move {
        start_server_with_config(&config).await.unwrap();
    });

    time::sleep(Duration::from_millis(10)).await;
    let mut config: ClientConfig = toml::from_str(include_str!("../fixtures/client.conf"))?;
    config.general.addr = addr.into();

    let stream = start_client_with_config(&config).await?;
    let mut client = ProstClientStream::new(stream);

    // 生成一个 HSET 命令
    let cmd = CommandRequest::new_hset("table1", "hello", "world".to_string().into());
    client.execute(cmd).await?;

    // 生成一个 HGET 命令
    let cmd = CommandRequest::new_hget("table1", "hello");
    let data = client.execute(cmd).await?;

    assert_eq!(data.status, 200);
    assert_eq!(data.values, &["world".into()]);

    Ok(())
}
