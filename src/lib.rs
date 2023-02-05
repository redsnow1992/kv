mod config;
mod error;
mod network;
mod pb;
mod service;
mod storage;

use anyhow::Result;
pub use config::*;
pub use error::KvError;
pub use network::*;
pub use pb::abi::*;
pub use service::*;
pub use storage::*;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::client;
use tracing::info;

pub async fn start_server_with_config(config: &ServerConfig) -> Result<()> {
    let acceptor =
        TlsServerAcceptor::new(&config.tls.cert, &config.tls.key, config.tls.ca.as_deref())?;
    let addr = &config.general.addr;

    match &config.storage {
        StorageConfig::MemTable => start_tls_server(addr, MemTable::new(), acceptor).await?,
        StorageConfig::SledDb(path) => start_tls_server(addr, SledDb::new(path), acceptor).await?,
    };

    Ok(())
}

pub async fn start_client_with_config(
    config: &ClientConfig,
) -> Result<client::TlsStream<TcpStream>> {
    let addr = &config.general.addr;
    let tls = &config.tls;

    let identity = tls.identity.as_ref().map(|(c, k)| (c.as_str(), k.as_str()));
    let connector = TlsClientConnector::new(&tls.domain, identity, tls.ca.as_deref())?;
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;

    Ok(stream)
}

async fn start_tls_server<Store: Storage>(
    addr: &str,
    store: Store,
    acceptor: TlsServerAcceptor,
) -> Result<()> {
    let service: Service<Store> = ServiceInner::new(store).into();
    let listener = TcpListener::bind(addr).await?;

    info!("Start listening on {}", addr);
    loop {
        let tls = acceptor.clone();
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);

        let stream = tls.accept(stream).await?;
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move { stream.process().await });
    }
}
