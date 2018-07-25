use super::rdkafka::message::ToBytes;

pub trait KeyGenerator {
    type Item: ToBytes + ?Sized;

    fn generate(&self, msg: &String) -> Self::Item;
}

pub struct StringKeyGenerator;

impl KeyGenerator for StringKeyGenerator {
    type Item = String;

    fn generate(&self, msg: &String) -> Self::Item {
        msg.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_generator() {
        assert_eq!(StringKeyGenerator.generate(&"test".to_string()), "test".to_string());
    }
}