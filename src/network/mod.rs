use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::info;

use crate::{CommandRequest, CommandResponse, KvError, Service, Storage};

mod frame;
mod tls;

pub use frame::{read_frame, FrameCoder};
pub use tls::{TlsClientConnector, TlsServerAcceptor};

pub struct ProstServerStream<S, Store> {
    inner: S,
    service: Service<Store>,
}

pub struct ProstClientStream<S> {
    inner: S,
}

impl<S, Store> ProstServerStream<S, Store>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    Store: Storage,
{
    pub fn new(stream: S, service: Service<Store>) -> Self {
        Self {
            inner: stream,
            service,
        }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        while let Ok(cmd) = self.recv().await {
            info!("Got a command: {:?}", cmd);
            let res = self.service.execute(cmd);
            self.send(res).await?;
        }
        Ok(())
    }

    async fn send(&mut self, msg: CommandResponse) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        msg.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded[..]).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<CommandRequest, KvError> {
        let mut buf = BytesMut::new();
        let stream = &mut self.inner;
        read_frame(stream, &mut buf).await?;
        CommandRequest::decode_frame(&mut buf)
    }
}

impl<S> ProstClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self { inner: stream }
    }

    pub async fn execute(&mut self, cmd: CommandRequest) -> Result<CommandResponse, KvError> {
        self.send(cmd).await?;
        self.recv().await
    }

    async fn send(&mut self, msg: CommandRequest) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        msg.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded[..]).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<CommandResponse, KvError> {
        let mut buf = BytesMut::new();
        let stream = &mut self.inner;
        read_frame(stream, &mut buf).await?;
        CommandResponse::decode_frame(&mut buf)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use anyhow::Result;
    use bytes::Bytes;
    use tokio::net::{TcpListener, TcpStream};

    use crate::{
        assert_res_ok, network::ProstClientStream, CommandRequest, MemTable, Service, ServiceInner,
        Value,
    };

    use super::ProstServerStream;

    #[tokio::test]
    async fn client_server_basic_communication_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let res = client.execute(cmd).await.unwrap();

        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = client.execute(cmd).await.unwrap();

        assert_res_ok(res, &["v1".into()], &[]);

        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        let v: Value = Bytes::from(vec![0u8; 16384]).into();
        let cmd = CommandRequest::new_hset("t2", "k2", v.clone());
        let res = client.execute(cmd).await.unwrap();

        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("t2", "k2");
        let res = client.execute(cmd).await.unwrap();

        assert_res_ok(res, &[v], &[]);

        Ok(())
    }

    async fn start_server() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let service: Service = ServiceInner::new(MemTable::new()).into();
                let server = ProstServerStream::new(stream, service);
                tokio::spawn(server.process());
            }
        });

        Ok(addr)
    }
}
