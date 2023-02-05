use std::time::Duration;

use anyhow::Result;
use criterion::{criterion_group, criterion_main, Criterion};
use kv::{
    start_client_with_config, start_server_with_config, ClientConfig, CommandRequest,
    ProstClientStream, ServerConfig, StorageConfig,
};
use rand::seq::SliceRandom;
use tokio::{net::TcpStream, runtime::Builder, time};
use tokio_rustls::client;
use tracing::{info, span};
use tracing_subscriber::{prelude::*, EnvFilter};

async fn start_server() -> Result<()> {
    let addr = "127.0.0.1:9999";
    let mut config: ServerConfig = toml::from_str(include_str!("../fixtures/server.conf"))?;
    config.general.addr = addr.into();
    config.storage = StorageConfig::MemTable;

    tokio::spawn(async move {
        start_server_with_config(&config).await.unwrap();
    });

    Ok(())
}

async fn connect() -> Result<client::TlsStream<TcpStream>> {
    let addr = "127.0.0.1:9999";
    let mut config: ClientConfig = toml::from_str(include_str!("../fixtures/client.conf"))?;
    config.general.addr = addr.into();

    Ok(start_client_with_config(&config).await?)
}

async fn start_subscribers(topic: &'static str) -> Result<()> {
    let stream = connect().await?;
    let mut client = ProstClientStream::new(stream);
    info!("C(subscriber): stream opened");
    let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
    tokio::spawn(async move {
        // let mut stream = client.execute(cmd).await.unwrap();
        // while let Some(Ok(data)) = stream.next().await {
        //     drop(data);
        // }
        client.execute(cmd).await.unwrap();
    });

    Ok(())
}

async fn start_publishers(topic: &'static str, values: &'static [&'static str]) -> Result<()> {
    let mut rng = rand::thread_rng();
    let v = values.choose(&mut rng).unwrap();

    let stream = connect().await?;
    let mut client = ProstClientStream::new(stream);
    info!("C(publisher): stream opened");

    let cmd = CommandRequest::new_hset("t1", "k1", (*v).into());
    client.execute(cmd).await.unwrap();

    Ok(())
}

fn pubsub(c: &mut Criterion) {
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("kv-bench")
        .install_simple()
        .unwrap();
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(opentelemetry)
        .init();

    let root = span!(tracing::Level::INFO, "app_start", work_units = 2);
    let _enter = root.enter();
    // 创建 Tokio runtime
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("pubsub")
        .enable_all()
        .build()
        .unwrap();

    let base_str = include_str!("../fixtures/server.conf"); // 891 bytes

    let values: &'static [&'static str] = Box::leak(
        vec![
            &base_str[..64],
            &base_str[..128],
            &base_str[..256],
            &base_str[..512],
        ]
        .into_boxed_slice(),
    );
    let topic = "lobby";

    // // 运行服务器和 100 个 subscriber，为测试准备
    // runtime.block_on(async {
    //     eprint!("preparing server and subscribers");
    //     start_server().await.unwrap();
    //     time::sleep(Duration::from_millis(50)).await;
    //     for _ in 0..1000 {
    //         start_subscribers(topic).await.unwrap();
    //         eprint!(".");
    //     }
    //     eprintln!("Done!");
    // });

    // 进行 benchmark
    c.bench_function("publishing", move |b| {
        b.to_async(&runtime)
            .iter(|| async { start_publishers(topic, values).await })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = pubsub
}
criterion_main!(benches);
