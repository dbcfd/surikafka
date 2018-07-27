use super::bytes;
use super::errors::Error;
use super::futures::{
    Async,
    Poll,
    Stream
};
use super::tokio::io::AsyncRead;

pub struct EveReader<T: AsyncRead> {
    inner: T,
    buffer: bytes::BytesMut
}

impl<T: AsyncRead> EveReader<T> {
    pub fn new(inner: T) -> EveReader<T> {
        EveReader {
            inner: inner,
            buffer: bytes::BytesMut::with_capacity(10_000_000)
        }
    }
}

const EOL: u8 = '\n' as u8;
const END_PAREN: u8 = '}' as u8;

impl<T: AsyncRead> Stream for EveReader<T> {
    type Item = String;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        let bytes_read = try_ready!(self.inner.read_buf(&mut self.buffer));

        if bytes_read == 0 && self.buffer.is_empty() {
            debug!("No bytes read, eve stream complete");
            Ok(Async::Ready(None))
        } else {
            debug!("Checking buffer after reading {} bytes", bytes_read);

            let mut located_at = None;

            for idx in 0..self.buffer.len() {
                if let Some(_) = located_at {
                    if self.buffer[idx] == EOL {
                        trace!("Located eol at {}", idx);
                        located_at = Some(idx);
                        break;
                    } else {
                        trace!("Expected eol, but none seen");
                        located_at = None;
                    }
                } else {
                    if self.buffer[idx] == END_PAREN {
                        trace!("Located end parentheses at {}", idx);
                        located_at = Some(idx);
                    }
                }
            }

            if let Some(at) = located_at {
                debug!("Splitting buffer at {}", at);
                let mut to_send = self.buffer.split_to(at + 1);
                to_send.truncate(at);
                let to_send_str = String::from_utf8(to_send.to_vec())?;
                debug!("Sending {}", to_send_str);
                Ok(Async::Ready(Some(to_send_str)))
            } else if bytes_read == 0 {
                debug!("Did not locate a string, and no bytes were read, completing");
                Ok(Async::Ready(None))
            } else {
                debug!("No string located");
                Ok(Async::NotReady)
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
                        trace!("Writing {} to buffer at {}", (b as char), bytes_written);
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
            let s1 = "{\"key1\":\"key with a return \n\",\"key2\":123".as_bytes().iter().fold(init, |s, b| {
                let byte = b.clone();
                Box::new(s.and_then(move |send| {
                    send.send(byte)
                }))
            });
            let s2 = "45}\n{\"another\":\"part ".as_bytes().iter().fold(s1, |s, b| {
                let byte = b.clone();
                Box::new(s.and_then(move |send| send.send(byte)))
            });
            let f = "being sent\"}\n".as_bytes().iter().fold(s2, |s, b| {
                let byte = b.clone();
                Box::new(s.and_then(move |send| send.send(byte)))
            });

            let mut final_sender = f.wait().expect("Failed to send");

            info!("Send complete");

            final_sender.close().into_future().wait().expect("Failed to close")
        });

        let strings = rt.block_on(fut_strings).expect("Failed to receive strings");

        assert_eq!(strings, vec![
            "{\"key1\":\"key with a return \n\",\"key2\":12345}".to_string(),
            "{\"another\":\"part being sent\"}".to_string()
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

        let fut_strings = EveReader::new(eve_receiver)
            .collect();

        let send_complete = std::thread::spawn(move || {
            let init: Box<Future<Item=mpsc::Sender<u8>, Error=mpsc::SendError<u8>>> = Box::new(futures::finished(sender));
            let f = "{\"key1\":\"key without a return\"}\n".as_bytes().iter().fold(init, |s, b| {
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
            "{\"key1\":\"key without a return\"}".to_string()
        ]);

        send_complete.join().expect("Failed to send");
    }
}