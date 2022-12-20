pub(crate) struct Value {
    value: String,
    timestamp: u64,
}

impl Value {
    pub(crate) fn new(value: String, timestamp: u64) -> Value {
        return Value {
            value,
            timestamp,
        };
    }

    pub(crate) fn get_value(&self) -> String {
        return self.value.clone();
    }
}