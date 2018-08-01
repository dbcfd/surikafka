#![recursion_limit="128"]
#![feature(try_from, test)]
#![allow(dead_code)]
extern crate bytes;
extern crate env_logger;
#[macro_use] extern crate error_chain;
#[macro_use] extern crate futures;
#[macro_use(debug, info, error, log, trace, warn)] extern crate log;
//#[macro_use] extern crate nom;
extern crate serde;
extern crate serde_json;
extern crate rdkafka;
#[macro_use] extern crate structopt;
extern crate tokio;
extern crate tokio_uds;

pub mod errors {
    use std;
    use super::{
        futures,
        //nom
    };

    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {
        foreign_links {
            Canceled(futures::Canceled) #[doc = "Future cancelled"];
            Io(std::io::Error) #[doc = "Error during IO"];
            Ffi(std::ffi::NulError) #[doc = "Error during FFI conversion"];
            FromUtf8(std::string::FromUtf8Error) #[doc = "Error during UTF8 conversion"];
            TimeError(std::time::SystemTimeError) #[doc = "Error during duration calculation"];
            Utf8(std::str::Utf8Error) #[doc = "Error during UTF8 conversion"];
        }
//        links {
//            ErrorName(error_lib::errors::Error, error_lib::errors::ErrorKind);
//        }
        errors {
            ReceiverError {
                display("Receiver encountered an error")
            }
            NomIncomplete(needed: String) {
                display("Not enough data to parse, needed {}", needed)
            }
            NomError(message: String) {
                display("Error parsing: {}", message)
            }
        }
    }

//    impl<I, E> From<nom::Err<I, E>> for Error {
//        fn from(err: nom::Err<I, E>) -> Error {
//            match err {
//                nom::Err::Incomplete(super::nom::Needed::Unknown) => {
//                    Error::from_kind(ErrorKind::NomIncomplete("Unknown".to_string()))
//                }
//                nom::Err::Incomplete(super::nom::Needed::Size(sz)) => {
//                    Error::from_kind(ErrorKind::NomIncomplete(format!("{}", sz)))
//                }
//                nom::Err::Error(super::nom::simple_errors::Context::Code(_, k)) => {
//                    Error::from_kind(ErrorKind::NomError(k.description().to_string()))
//                }
//                nom::Err::Failure(super::nom::simple_errors::Context::Code(_, k)) => {
//                    Error::from_kind(ErrorKind::NomError(k.description().to_string()))
//                }
//            }
//        }
//    }
}

mod json;
mod key;
mod reader;
mod stats;
mod writer;

#[derive(Debug, StructOpt, Clone)]
pub struct CommandLineArguments {
    #[structopt(long = "eve", short = "e", default_value="/tmp/suricata.alerts")]
    eve_socket_path: String,
    #[structopt(long = "kafka", short = "k", default_value="kafka:9092")]
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

fn run_main(args: CommandLineArguments) -> Result<(), Error> {
    let mut rt = tokio::runtime::Runtime::new().map_err(Error::from)?;

    let producer: rdkafka::producer::FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", args.kafka_servers.as_str())
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let uds_path = std::path::PathBuf::from(args.eve_socket_path);

    if uds_path.exists() {
        std::fs::remove_file(uds_path.clone()).map_err(Error::from)?
    }

    let listener = tokio_uds::UnixListener::bind(uds_path).map_err(Error::from)?;

    let stream_res = listener.incoming()
        .map_err(Error::from)
        .map(|s| {
            debug!("Stream connected at {:?}", s.peer_addr());
            reader::EveReader::new(s)
        }).flatten()
        .produce(
            args.topic.clone(),
            key::BytesGenerator,
            producer
        ).for_each(|_| {
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
