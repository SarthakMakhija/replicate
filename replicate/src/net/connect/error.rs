use std::error::Error;

pub type ServiceResponseError = Box<dyn Error + Send + Sync + 'static>;

pub type AnyError = Box<dyn Error + Send + Sync + 'static>;