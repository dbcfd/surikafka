use super::rdkafka::message::ToBytes;

pub trait KeyGenerator {
    type Item: ToBytes + ?Sized;

    fn generate(&self, msg: &Vec<u8>) -> Self::Item;
}

pub struct BytesGenerator;

impl KeyGenerator for BytesGenerator {
    type Item = Vec<u8>;

    fn generate(&self, msg: &Vec<u8>) -> Self::Item {
        msg.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_generator() {
        assert_eq!(BytesGenerator.generate(&"test".to_string().into_bytes()), "test".to_string().into_bytes());
    }
}