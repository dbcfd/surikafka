use super::errors::{
    Error
};
use super::futures::{
    Async,
    Future,
    Poll,
    Stream
};
use super::key::KeyGenerator;
use super::rdkafka::{
    ClientContext,
    producer::{
        DeliveryFuture,
        FutureProducer,
        FutureRecord
    }
};

pub struct Writer<C, K, S>
    where C: ClientContext + 'static,
    K: KeyGenerator,
    S: Stream<Item=String, Error=Error>
{
    inner: S,
    topic: String,
    generator: K,
    producer: FutureProducer<C>,
    outstanding: Option<DeliveryFuture>
}

impl<C, K, S> Writer<C, K, S>
    where C: ClientContext + 'static,
          K: KeyGenerator,
          K::Item: Sized,
          S: Stream<Item=String, Error=Error>
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

    pub fn send(&mut self, msg: &String) -> DeliveryFuture {
        let key = self.generator.generate(msg);
        let record: FutureRecord<K::Item, String> = FutureRecord::to(self.topic.as_ref())
            .key(&key)
            .payload(&msg);
        self.producer.send(record, 1000)
    }
}

pub trait WithProduce<S> where S: Stream<Item=String, Error=Error> {
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

impl<S> WithProduce<S> for S where S: Stream<Item=String, Error=Error> {
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

impl<C, K, S> Stream for Writer<C, K, S>
    where C: ClientContext + 'static,
          K: KeyGenerator,
          K::Item: Sized,
          S: Stream<Item=String, Error=Error>
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        let outstanding_ready: Poll<Option<(i32, i64)>, Error> = if let Some(mut f) = self.outstanding.take() {
            let produce_attempt = try_ready!(f.poll());
            match produce_attempt {
                Err( (e, _) ) => {
                    error!("Failed to produce: {:?}", e);
                    Ok(Async::NotReady)
                }
                Ok( (p, o) ) => {
                    Ok(Async::Ready(Some( (p, o) )))
                }
            }
        } else {
            Ok(Async::Ready(None))
        };

        if let Some( (p, o) ) = try_ready!(outstanding_ready) {
            debug!("Produced to partition {}, offset {}", p, o);
            Ok(Async::Ready(Some(())))
        } else {
            trace!("Polling for next future");
            if let Some(msg) = try_ready!(self.inner.poll()) {
                self.outstanding = Some(self.send(&msg));
                Ok(Async::NotReady)
            } else {
                Ok(Async::Ready(None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use self::super::super::{
        key::StringKeyGenerator,
        env_logger,
        errors::ErrorKind,
        futures::{
            Sink,
            sync::mpsc as mpsc
        },
        rdkafka::{
            ClientConfig
        },
        tokio
    };
    use std;

    #[test]
    fn produces_messages() {
        let _ = env_logger::try_init();

        let mut rt = tokio::runtime::Runtime::new().expect("Failed to build runtime");

        let (sender, receiver) = mpsc::channel::<String>(100);

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");

        let send_finished = std::thread::spawn(move || {
            info!("Attempting to send items for publish");

            let mut post_send = sender.send("string1".to_string()).wait().expect("Failed to send")
                .send("string2".to_string()).wait().expect("Failed to send")
                .send("string3".to_string()).wait().expect("Failed to send");

            info!("Stopping sender");

            std::thread::sleep(std::time::Duration::from_secs(5));

            post_send.close().expect("Failed to close")
        });

        std::thread::sleep(std::time::Duration::from_secs(5));

        info!("Creating receiver future");

        let fut_result = receiver.map_err(|_| {
            error!("Error encountered while running receiver");
            Error::from_kind(ErrorKind::ReceiverError)
        })
            .produce("test_topic".to_string(), StringKeyGenerator, producer)
            .collect();

        let sent = rt.block_on(fut_result).expect("Failed to send");

        assert_eq!(sent.len(), 3);

        send_finished.join().expect("Failed to join");
    }
}