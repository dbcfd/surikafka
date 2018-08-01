use super::{
    bytes,
    errors::Error,
    futures::{
        Async,
        Poll,
        Stream
    },
    json,
    //nom,
    //json::JsonValue,
    tokio::io::AsyncRead
};

pub struct EveReader<T: AsyncRead> {
    inner: T,
    buffer: bytes::BytesMut,
    pending_alerts: Vec<Vec<u8>>
}

impl<T: AsyncRead> EveReader<T> {
    pub fn new(inner: T) -> EveReader<T> {
        EveReader {
            inner: inner,
            buffer: bytes::BytesMut::with_capacity(10_000_000),
            pending_alerts: vec![]
        }
    }

    pub fn collect_alerts(&mut self) -> Result<(), Error> {
        let (rem, mut alerts) = json::JsonParser::parse(self.buffer.as_ref())?;

        self.pending_alerts.append(&mut alerts);
        self.buffer.split_at(self.buffer.len() - rem.len());

        Ok( () )
    }
}

impl<T: AsyncRead> Stream for EveReader<T> {
    type Item = Vec<u8>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        if let Some(v) = self.pending_alerts.pop() {
            Ok(Async::Ready(Some(v)))
        } else {
            let bytes_read = try_ready!(self.inner.read_buf(&mut self.buffer));

            if bytes_read == 0 {
                Ok(Async::Ready(None))
            } else {
                debug!("Checking buffer after reading {} bytes", bytes_read);

                self.collect_alerts()?;

                if let Some(v) = self.pending_alerts.pop() {
                    Ok(Async::Ready(Some(v)))
                } else {
                    Ok(Async::Ready(None))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use self::super::super::{
        env_logger,
        futures,
        futures::{
            Future,
            IntoFuture,
            Sink,
            Stream,
            sync::mpsc as mpsc
        },
        tokio
    };
    use std;

    struct ReceiverReader {
        inner: mpsc::Receiver<u8>
    }

    impl std::io::Read for ReceiverReader {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
            debug!("Reading receiver");
            loop {
                if let Async::Ready(bytes) = self.poll_read(buf)? {
                    return Ok(bytes)
                }
            }
        }
    }

    impl tokio::io::AsyncRead for ReceiverReader {
        fn poll_read(&mut self, buf: &mut [u8]) -> Poll<usize, std::io::Error> {
            let mut bytes_written: usize = 0;
            loop {
                match self.inner.poll().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Interrupted, "Receiver failed")
                })? {
                    Async::NotReady => {
                        debug!("Not ready for read");
                        if bytes_written == 0 {
                            return Ok(Async::NotReady)
                        } else {
                            break
                        }
                    },
                    Async::Ready(None) => {
                        if bytes_written == 0 {
                            debug!("No bytes written, and no more will be received");
                            return Ok(Async::Ready(0))
                        } else {
                            break
                        }
                    }
                    Async::Ready(Some(b)) => {
                        buf[bytes_written] = b;
                        bytes_written += 1;
                        if bytes_written == buf.len() {
                            break
                        }
                    }
                }
            }

            Ok(Async::Ready(bytes_written))
        }
    }

    #[test]
    fn reads_eve() {
        let _ = env_logger::try_init();

        let mut rt = tokio::runtime::Runtime::new().expect("Failed to build runtime");

        let (sender, receiver) = mpsc::channel::<u8>(1000);

        let eve_receiver = ReceiverReader {
            inner: receiver
        };

        let fut_strings = EveReader::new(eve_receiver)
            .collect();

        let send_complete = std::thread::spawn(move || {
            let init: Box<Future<Item=mpsc::Sender<u8>, Error=mpsc::SendError<u8>>> = Box::new(futures::finished(sender));
            let s1 = "{\"key1\":\"key with a paren set {}\",\"key2\":123".as_bytes().iter().fold(init, |s, b| {
                let byte = b.clone();
                Box::new(s.and_then(move |send| {
                    send.send(byte)
                }))
            });
            let s2 = "45}{\"another\":\"part ".as_bytes().iter().fold(s1, |s, b| {
                let byte = b.clone();
                Box::new(s.and_then(move |send| send.send(byte)))
            });
            let f = "being sent\"}".as_bytes().iter().fold(s2, |s, b| {
                let byte = b.clone();
                Box::new(s.and_then(move |send| send.send(byte)))
            });

            let mut final_sender = f.wait().expect("Failed to send");

            info!("Send complete");

            final_sender.close().into_future().wait().expect("Failed to close")
        });

        let strings = rt.block_on(fut_strings).expect("Failed to receive strings");

        strings.iter().for_each(|s| {
            debug!("Received {:?}", String::from_utf8(s.to_vec()));
        });

        assert_eq!(strings, vec![
            "{\"another\":\"part being sent\"}".to_string().into_bytes(),
            "{\"key1\":\"key with a paren set {}\",\"key2\":12345}".to_string().into_bytes()
        ]);

        send_complete.join().expect("Failed to send");
    }
    #[test]
    fn reads_single_eve_event() {
        let _ = env_logger::try_init();

        let mut rt = tokio::runtime::Runtime::new().expect("Failed to build runtime");

        let (sender, receiver) = mpsc::channel::<u8>(1000);

        let eve_receiver = ReceiverReader {
            inner: receiver
        };

        let fut_strings = EveReader::new(eve_receiver).collect();

        let send_complete = std::thread::spawn(move || {
            let init: Box<Future<Item=mpsc::Sender<u8>, Error=mpsc::SendError<u8>>> = Box::new(futures::finished(sender));
            let f = "{\"key1\":\"key without a return\"}".as_bytes().iter().fold(init, |s, b| {
                let byte = b.clone();
                Box::new(s.and_then(move |send| {
                    send.send(byte)
                }))
            });

            let mut final_sender = f.wait().expect("Failed to send");

            info!("Send complete");

            final_sender.close().into_future().wait().expect("Failed to close")
        });

        let strings = rt.block_on(fut_strings).expect("Failed to receive strings");

        assert_eq!(strings, vec![
            "{\"key1\":\"key without a return\"}".to_string().into_bytes()
        ]);

        send_complete.join().expect("Failed to send");
    }
}