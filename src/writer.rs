use super::{
    futures,
    futures::{
        Async,
        Future,
        Poll,
        Stream
    },
    key::KeyGenerator,
    rdkafka::{
        ClientContext,
        producer::{
            DeliveryFuture,
            FutureProducer,
            FutureRecord
        }
    },
    stats
};
use std;

struct OutstandingProduce {
    alert_length: usize,
    sent_at: std::time::Instant,
    future_produce: DeliveryFuture
}

pub struct Writer<C, K, S>
    where C: ClientContext + 'static,
          K: KeyGenerator,
          S: Stream,
          S::Error: std::convert::From<futures::Canceled>,
          S::Item: std::convert::AsRef<Vec<u8>>
{
    inner: S,
    topic: String,
    generator: K,
    producer: FutureProducer<C>,
    outstanding: Option<OutstandingProduce>
}

impl<C, K, S> Writer<C, K, S>
    where C: ClientContext + 'static,
          K: KeyGenerator,
          K::Item: Sized,
          S: Stream,
          S::Error: std::convert::From<futures::Canceled>,
          S::Item: std::convert::AsRef<Vec<u8>>
{
    pub fn new(
        stream: S,
        topic: String,
        generator: K,
        producer: FutureProducer<C>
    ) -> Writer<C, K, S> {
        Writer {
            inner: stream,
            topic: topic,
            generator: generator,
            producer: producer,
            outstanding: None
        }
    }

    pub fn send(&mut self, msg: &Vec<u8>) -> DeliveryFuture {
        let key = self.generator.generate(msg);
        let record: FutureRecord<K::Item, Vec<u8>> = FutureRecord::to(self.topic.as_ref())
            .key(&key)
            .payload(msg);
        self.producer.send(record, 1000)
    }

    fn poll_outstanding(&mut self) -> Poll<Option<OutstandingProduce>, S::Error> {
        if let Some(mut outstanding) = self.outstanding.take() {
            trace!("Checking outstanding future");
            match outstanding.future_produce.poll()? {
                Async::NotReady => {
                    debug!("Not ready, will poll later");
                    self.outstanding = Some(outstanding);
                    Ok(Async::NotReady)
                }
                Async::Ready(Err( (e, _) )) => {
                    error!("Failed to produce: {:?}", e);
                    Ok(Async::NotReady)
                }
                Async::Ready(Ok( (p, o) )) => {
                    debug!("Produced to partition {}, offset {}", p, o);

                    Ok(Async::Ready(Some(outstanding)))
                }
            }
        } else {
            trace!("No outstanding future, will poll for next future");
            Ok(Async::Ready(None))
        }
    }
}

impl<C, K, S> Stream for Writer<C, K, S>
    where C: ClientContext + 'static,
          K: KeyGenerator,
          K::Item: Sized,
          S: Stream,
          S::Error: std::convert::From<futures::Canceled>,
          S::Item: std::convert::AsRef<Vec<u8>>
{
    type Item = stats::Stats;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut current_stats = stats::Stats::default();
        loop {
            if let Some(outstanding) = try_ready!(self.poll_outstanding()) {
                current_stats.mark(
                    outstanding.alert_length,
                    std::time::Instant::now() - outstanding.sent_at
                );
            } else {
                match self.inner.poll()? {
                    Async::Ready(Some(msg)) => {
                        let oustanding = OutstandingProduce {
                            alert_length: msg.as_ref().len(),
                            sent_at: std::time::Instant::now(),
                            future_produce: self.send(msg.as_ref())
                        };
                        self.outstanding = Some(oustanding);
                    }
                    Async::NotReady => {
                        debug!("No messages ready to send");
                        if current_stats.alert_count() > 0 {
                            return Ok(Async::Ready(Some(current_stats)));
                        } else {
                            return Ok(Async::NotReady)
                        }
                    }
                    Async::Ready(None) => {
                        debug!("No more messages available");
                        if current_stats.alert_count() > 0 {
                            return Ok(Async::Ready(Some(current_stats)));
                        } else {
                            return Ok(Async::Ready(None))
                        }
                    }

                }
            }
        }
    }
}

pub trait WithProduce<S>
    where S: Stream + Sized,
          S::Error: std::convert::From<futures::Canceled>,
          S::Item: std::convert::AsRef<Vec<u8>>
{
    fn produce<C, K>(
        self,
        topic: String,
        generator: K,
        producer: FutureProducer<C>
    ) -> Writer<C, K, S>
        where C: ClientContext + 'static,
              K: KeyGenerator,
              K::Item: Sized;
}

impl<S> WithProduce<S> for S
    where S: Stream,
          S::Error: std::convert::From<futures::Canceled>,
          S::Item: std::convert::AsRef<Vec<u8>>
{
    fn produce<C, K>(
        self,
        topic: String,
        generator: K,
        producer: FutureProducer<C>
    ) -> Writer<C, K, S>
        where C: ClientContext + 'static,
              K: KeyGenerator,
              K::Item: Sized
    {
        Writer::new(self, topic, generator, producer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use self::super::super::{
        key::BytesGenerator,
        env_logger,
        errors::{
            Error
        },
        rdkafka::{
            ClientConfig
        },
        tokio
    };

    #[test]
    fn produces_messages() {
        let _ = env_logger::try_init();

        let mut rt = tokio::runtime::Runtime::new().expect("Failed to build runtime");

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");

        let send_stream = futures::stream::iter_ok::<Vec<Vec<u8>>, Error>(
            vec![
                "string1".to_string().into_bytes(),
                "string2".to_string().into_bytes(),
                "string3".to_string().into_bytes()
            ]
        );

        let fut_result = send_stream
            .produce("test_topic".to_string(), BytesGenerator, producer)
            .collect();

        let sent = rt.block_on(fut_result).expect("Failed to send");

        assert_eq!(sent.len(), 3);
    }
}