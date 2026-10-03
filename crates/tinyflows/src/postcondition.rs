//! Stub.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::data::Item;

/// Stub.
pub const REQUIREMENTS: [&str; 3] = ["non_empty", "field_present", "non_empty_list"];

/// Stub.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Postcondition {
    /// Stub.
    #[serde(default)]
    pub require: String,
    /// Stub.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

impl Postcondition {
    /// Stub.
    #[must_use]
    pub fn from_config(_config: &Value) -> Option<Result<Self, String>> {
        None
    }
    /// Stub.
    pub fn check(&self, _output: &Value) -> Result<(), String> {
        Ok(())
    }
    /// Stub.
    pub fn check_items(&self, _items: &[Item]) -> Result<(), String> {
        Ok(())
    }
    /// Stub.
    pub fn validate(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "postcondition_tests.rs"]
mod tests;
