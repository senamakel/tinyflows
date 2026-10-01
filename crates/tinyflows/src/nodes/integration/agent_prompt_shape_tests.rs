//! Request-shape edge cases for the agent prompt helpers: array output
//! schemas and an empty `messages` array alongside a `prompt`.

use super::*;
use serde_json::json;

#[test]
fn structured_output_instruction_names_an_array_for_an_array_schema() {
    let request = json!({ "output_parser": { "schema": { "type": "array" } } });
    let inst = structured_output_instruction(&request).expect("instruction");
    assert!(inst.starts_with("Respond with a single JSON array only"), "{inst}");
    assert!(inst.contains("The array must match this JSON Schema"), "{inst}");
    assert!(!inst.contains("object"), "{inst}");
}

#[test]
fn structured_output_instruction_keeps_object_wording_without_a_schema_type() {
    let request = json!({ "response_format": "json" });
    let inst = structured_output_instruction(&request).expect("instruction");
    assert!(inst.starts_with("Respond with a single JSON object only"), "{inst}");
}

#[test]
fn prepend_system_message_seeds_prompt_when_messages_is_empty() {
    let mut req = json!({ "prompt": "x", "messages": [] });
    prepend_system_message(&mut req, "persona");
    let messages = req["messages"].as_array().expect("messages");
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["role"], "system");
    assert_eq!(messages[1]["role"], "user");
    assert_eq!(messages[1]["content"], "x");
}
