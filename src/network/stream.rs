use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures::{Stream, Sink, ready, FutureExt};
use tokio::io::{AsyncRead, AsyncWrite};
use anyhow::Result;

use crate::{FrameCoder, KvError, read_frame};

pub struct ProstStream<S, In, Out> where {
    stream: S,
    wbuf: BytesMut,
    written: usize,
    rbuf: BytesMut,
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<S, In, Out> ProstStream<S, In, Out> 
where
    S: AsyncRead + AsyncWrite + Send + Unpin
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            written: 0,
            wbuf: BytesMut::new(),
            rbuf: BytesMut::new(),
            _in: PhantomData::default(),
            _out: PhantomData::default(),
        }
    }
}

impl<S, Req, Res> Unpin for ProstStream<S, Req, Res> where S: Unpin {}

impl<S, In, Out> Stream for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send + FrameCoder,
    Out: Unpin + Send,
{
    type Item = Result<In, KvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(self.rbuf.len() == 0);

        let mut rest = self.rbuf.split_off(0);
        let fut = read_frame(&mut self.stream, &mut rest);
        ready!(Box::pin(fut).poll_unpin(cx))?;

        self.rbuf.unsplit(rest);
        Poll::Ready(Some(In::decode_frame(&mut self.rbuf)))
    }    
}

impl<S, In, Out> Sink<Out> for ProstStream<S, In, Out> 
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send,
    Out: Unpin + Send + FrameCoder,
{
    type Error = KvError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Out) -> Result<(), Self::Error> {
        let this = self.get_mut();
        item.encode_frame(&mut this.wbuf)?;

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        while this.written != this.wbuf.len() {
            let n = ready!(Pin::new(&mut this.stream)
                .poll_write(cx, &this.wbuf[this.written..]))?;
            this.written += n;
        }

        this.wbuf.clear();
        this.written = 0;


        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;

        ready!(Pin::new(&mut self.stream).poll_shutdown(cx))?;
        
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{utils::DummyStream, CommandRequest};
    use anyhow::Result;
    use futures::prelude::*;

    #[tokio::test]
    async fn prost_stream_should_work() -> Result<()> {
        let buf = BytesMut::new();
        let stream = DummyStream { buf };
        let mut stream = ProstStream::<_, CommandRequest, CommandRequest>::new(stream);
        let cmd = CommandRequest::new_hdel("t1", "k1");
        stream.send(cmd.clone()).await?;
        if let Some(Ok(s)) = stream.next().await {
            assert_eq!(s, cmd);
        } else {
            assert!(false);
        }

        Ok(())
    }
}