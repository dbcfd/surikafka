use errors::Error;
use serde_json::{
    Deserializer,
    Value
};

pub struct JsonParser;

impl JsonParser {
    pub fn parse<'a>(buffer: &'a [u8]) -> Result<(&'a [u8], Vec<Vec<u8>>), Error> {
        let deserializer = Deserializer::from_slice(buffer);
        let mut stream_deserializer = deserializer.into_iter::<Value>();
        let mut values = vec![];
        let mut last_good_offset = 0;

        loop {
            match stream_deserializer.next() {
                Some(Ok(_)) => {
                    let slice = buffer[last_good_offset..stream_deserializer.byte_offset()].to_vec();
                    values.push(slice);
                    last_good_offset = stream_deserializer.byte_offset();
                }
                Some(Err(_)) => {
                    return Ok( (&buffer[last_good_offset..buffer.len()], values) );
                }
                None => {
                    return Ok( (&buffer[last_good_offset..buffer.len()], values) );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use env_logger;
    use rdkafka::message::ToBytes;

    #[test]
    fn read_single_object() {
        let _ = env_logger::try_init();

        let test = r#"{"thiskey":"is for this object", "fkey": 23.4, "even":{"with":"inner"}}"#;

        let (rem, v) = JsonParser::parse(test.as_ref()).expect("Failed to parse");

        assert!(rem.is_empty());

        assert!(!v.is_empty());

        assert_eq!(v[0], test.to_bytes().to_vec());
    }

    #[test]
    fn read_partial_objects() {
        let _ = env_logger::try_init();

        let test = r#"{"key1":"key with a paren set {}","key2":12345}{"another":"part"#;

        let (rem, v) = JsonParser::parse(test.as_ref()).expect("Failed to parse");

        assert!(!rem.is_empty());

        assert_eq!(v, vec![
            r#"{"key1":"key with a paren set {}","key2":12345}"#.to_bytes().to_vec()
        ]);
    }

    #[test]
    fn read_multiple_objects() {
        let _ = env_logger::try_init();

        let test = r#"{"key1":"key with a paren set {}","key2":12345}{"another":"part being sent"}"#.as_ref();

        let (rem, v) = JsonParser::parse(test).expect("Failed to parse");

        assert!(rem.is_empty());

        assert_eq!(v, vec![
            r#"{"key1":"key with a paren set {}","key2":12345}"#.to_bytes().to_vec(),
            r#"{"another":"part being sent"}"#.to_bytes().to_vec()
        ]);
    }

}