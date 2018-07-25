use std;

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
        let bytes_written = try_ready!(self.inner.read_buf(&mut self.buffer));

        if bytes_written == 0 {
            Ok(Async::Ready(None))
        } else {
            let mut located_at = None;

            for idx in 0..self.buffer.len() {
                if let Some(_) = located_at {
                    if self.buffer[idx] == EOL {
                        located_at = Some(idx);
                        break;
                    } else {
                        located_at = None;
                    }
                } else {
                    if self.buffer[idx] == END_PAREN {
                        located_at = Some(idx);
                    }
                }
            }

            if let Some(at) = located_at {
                let to_send = self.buffer.split_to(at + 1);
                let to_send_str = String::from_utf8(to_send.to_vec())?;
                Ok(Async::Ready(Some(to_send_str)))
            } else {
                Ok(Async::NotReady)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use self::super::super::{
        futures::{
            Sink,
            Stream,
            sync::mpsc as mpsc
        },
        tokio
    };

    struct ReceiverReader {
        inner: mpsc::Receiver<u8>
    }

    impl std::io::Read for ReceiverReader {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
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
                        if bytes_written == 0 {
                            return Ok(Async::NotReady)
                        } else {
                            break
                        }
                    },
                    Async::Ready(None) => {
                        if bytes_written == 0 {
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
        let mut rt = tokio::runtime::Runtime::new().expect("Failed to build runtime");

        let (sender, receiver) = mpsc::channel::<u8>(1000);

        let eve_receiver = ReceiverReader {
            inner: receiver
        };

        let fut_strings = EveReader::new(eve_receiver)
            .collect();

        let send_complete = std::thread::spawn(move || {
            r#"{"key1":"key with a return \n","key2":123"#.as_bytes().iter().for_each(|b| {
                sender.send(b.clone());
            });
            r#"45}\n{"another":"part "#.as_bytes().iter().for_each(|b| {
                sender.send(b.clone());
            });
            r#"being sent"}\n"#.as_bytes().iter().for_each(|b| {
                sender.send(b.clone());
            });
        });

        let strings = rt.block_on(fut_strings).expect("Failed to receive strings");

        assert_eq!(strings, vec![
            r#"{"key1":"key with a return \n","key2":12345}"#.to_string(),
            r#"{"another":"part being sent"}"#.to_string()
        ]);
    }
}