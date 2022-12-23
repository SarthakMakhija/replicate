pub struct Value {
    value: String,
    timestamp: u64,
}

impl Value {
    pub fn new(value: String, timestamp: u64) -> Value {
        return Value {
            value,
            timestamp,
        };
    }

    pub(crate) fn get_value(&self) -> String {
        return self.value.clone();
    }

    pub(crate) fn get_timestamp(&self) -> u64 {
        return self.timestamp;
    }
}