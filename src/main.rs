#![recursion_limit="128"]
#![feature(try_from, test)]
#![allow(dead_code)]
extern crate bytes;
extern crate env_logger;
#[macro_use] extern crate error_chain;
#[macro_use] extern crate futures;
#[macro_use(debug, info, error, log, trace, warn)] extern crate log;
extern crate rdkafka;
#[macro_use] extern crate structopt;
extern crate tokio;
extern crate tokio_uds;

pub mod errors {
    use std;
    use super::futures;

    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {
        foreign_links {
            Io(std::io::Error) #[doc = "Error during IO"];
            Ffi(std::ffi::NulError) #[doc = "Error during FFI conversion"];
            Utf8(std::str::Utf8Error) #[doc = "Error during UTF8 conversion"];
            FromUtf8(std::string::FromUtf8Error) #[doc = "Error during UTF8 conversion"];
            TimeError(std::time::SystemTimeError) #[doc = "Error during duration calculation"];
            Canceled(futures::Canceled) #[doc = "Future cancelled"];
        }
//        links {
//            ErrorName(error_lib::errors::Error, error_lib::errors::ErrorKind);
//        }
//        errors {
//
//        }
    }
}

mod key;
mod reader;
mod writer;

#[derive(Debug, StructOpt, Clone)]
pub struct CommandLineArguments {
    #[structopt(long = "eve", short = "e", default_value="/tmp/suricata/alert.socket")]
    eve_socket_path: String,
    #[structopt(long = "kafka", short = "k", default_value="staging-kafka-001:2181,staging-kafka-002:2181")]
    kafka_servers: String,
    #[structopt(long = "topic", short = "t", default_value="eve-alerts")]
    topic: String
}

use errors::Error;
use futures::{
    Future,
    Stream
};
use structopt::StructOpt;
use writer::WithProduce;

fn print_error(err: &Error) {
    error!("error: {}", err);

    for e in err.iter().skip(1) {
        error!("caused by: {}", e);
    }

// The backtrace is not always generated. Try to run this example
// with `RUST_BACKTRACE=1`.
    if let Some(backtrace) = err.backtrace() {
        error!("backtrace: {:?}", backtrace)
    }
}

impl<S> writer::WithProduce<S> for S where S: Stream<Item=String, Error=Error> {
    fn produce<C, K>(
        self,
        topic: String,
        generator: K,
        producer: rdkafka::producer::FutureProducer<C>
    ) -> writer::Writer<C, K, S>
        where C: rdkafka::ClientContext + 'static,
              K: key::KeyGenerator,
              K::Item: Sized
    {
        writer::Writer::new(self, topic, generator, producer)
    }
}

fn run_main(args: CommandLineArguments) -> Result<(), Error> {
    let mut rt = tokio::runtime::Runtime::new().map_err(Error::from)?;

    let producer: rdkafka::producer::FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", args.kafka_servers.as_str())
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let uds_path = std::path::PathBuf::from(args.eve_socket_path);

    let listener = tokio_uds::UnixListener::bind(uds_path).map_err(Error::from)?;

    let stream_res = listener.incoming()
        .map(|s| {
            reader::EveReader::new(s)
        }).flatten()
        .produce(args.topic.clone(), key::StringKeyGenerator, producer)
        .for_each(|_| {
            Ok(())
        });

    let res = rt.block_on(stream_res)?;

    let _ = rt.shutdown_now().wait();

    Ok(res)
}

fn main() {
    let args = CommandLineArguments::from_args();

    let _ = env_logger::try_init();

    run_main(args).err().iter().for_each(print_error);

    info!("Exiting");
    ::std::process::exit(0);
}
