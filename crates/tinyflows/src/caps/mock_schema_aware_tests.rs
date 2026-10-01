use super::*;

// ── SchemaAwareMockAgentRunner ───────────────────────────────────────────

#[tokio::test]
async fn schema_aware_mock_agent_mirrors_the_echo_without_a_schema() {
    // No `output_parser.schema` on the request: identical shape to the
    // `MockAgentRunner` so schema-less dry runs are unaffected.
    let runner = SchemaAwareMockAgentRunner;
    let request = json!({ "prompt": "hi" });
    let out = runner
        .run_agent("researcher", request.clone(), Some("conn_1"))
        .await
        .expect("run_agent");
    assert_eq!(out["agent"], "researcher");
    assert_eq!(out["request"], request);
    assert_eq!(out["connection"], "conn_1");
}

#[tokio::test]
async fn schema_aware_mock_agent_populates_declared_properties() {
    let runner = SchemaAwareMockAgentRunner;
    let request = json!({
        "prompt": "extract",
        "output_parser": { "schema": { "type": "object",
            "required": ["email", "count", "active", "meta", "tags"],
            "properties": {
                "email": { "type": "string" },
                "count": { "type": "integer" },
                "active": { "type": "boolean" },
                "meta": { "type": "object" },
                "tags": { "type": "array" }
            } } }
    });
    let out = runner
        .run_agent("researcher", request, None)
        .await
        .expect("run_agent");
    assert_eq!(out["email"], "");
    assert_eq!(out["count"], 0);
    assert_eq!(out["active"], false);
    assert_eq!(out["meta"], json!({}));
    assert_eq!(out["tags"], json!([]));
}

#[tokio::test]
async fn schema_aware_mock_agent_populates_an_enum_property_with_an_allowed_value() {
    // A generic string placeholder (`""`) would fail the vendored
    // validator's `enum` check even though a real agent could easily
    // satisfy it — the mock must pick one of the schema's own allowed
    // values (see `placeholder_for_type`'s enum handling).
    let runner = SchemaAwareMockAgentRunner;
    let request = json!({
        "prompt": "triage",
        "output_parser": { "schema": { "type": "object",
            "required": ["priority"],
            "properties": {
                "priority": { "type": "string", "enum": ["urgent", "normal"] }
            } } }
    });
    let out = runner
        .run_agent("researcher", request, None)
        .await
        .expect("run_agent");
    let allowed = ["urgent", "normal"];
    assert!(
        allowed.contains(&out["priority"].as_str().unwrap()),
        "expected an allowed enum value, got: {out}"
    );
}

#[tokio::test]
async fn schema_aware_mock_agent_ignores_null_schema() {
    // `output_parser: { schema: null }` (or no `output_parser` at all) is
    // treated identically to "no schema" — the vendored echo shape.
    let runner = SchemaAwareMockAgentRunner;
    let request = json!({ "prompt": "hi", "output_parser": { "schema": null } });
    let out = runner
        .run_agent("researcher", request.clone(), None)
        .await
        .expect("run_agent");
    assert_eq!(out["agent"], "researcher");
    assert_eq!(out["request"], request);
}

// ── SchemaAwareMockLlm ───────────────────────────────────────────────────

#[tokio::test]
async fn schema_aware_mock_llm_mirrors_the_echo_without_a_schema() {
    // No `output_parser.schema`: byte-identical to the `MockLlm`
    // so schema-less agent dry runs (which route to the `llm` slot, not the
    // runner) keep today's `{ completion, connection }` shape.
    let llm = SchemaAwareMockLlm;
    let request = json!({ "prompt": "hi" });
    let out = llm
        .complete(request.clone(), Some("conn_1"))
        .await
        .expect("complete");
    assert_eq!(out["completion"], request);
    assert_eq!(out["connection"], "conn_1");

    let without_conn = llm.complete(request, None).await.expect("complete");
    assert!(without_conn["connection"].is_null());
}

#[tokio::test]
async fn schema_aware_mock_llm_synthesizes_a_schema_valid_completion() {
    // A plain agent node (no `agent_ref`) hands its config to the `llm`
    // slot; the returned object must pass the output-parser sub-port's
    // validator directly (no auto-fix hop) for every declared type.
    let llm = SchemaAwareMockLlm;
    let request = json!({
        "prompt": "extract",
        "output_parser": { "schema": { "type": "object",
            "required": ["email", "count", "active", "meta", "tags"],
            "properties": {
                "email": { "type": "string" },
                "count": { "type": "integer" },
                "active": { "type": "boolean" },
                "meta": { "type": "object" },
                "tags": { "type": "array" }
            } } }
    });
    let out = llm.complete(request, None).await.expect("complete");
    assert_eq!(out["email"], "");
    assert_eq!(out["count"], 0);
    assert_eq!(out["active"], false);
    assert_eq!(out["meta"], json!({}));
    assert_eq!(out["tags"], json!([]));
}

#[tokio::test]
async fn schema_aware_mock_llm_ignores_null_schema() {
    // `output_parser: { schema: null }` is treated as "no schema" — the
    // vendored echo shape, same as the runner's null-schema handling.
    let llm = SchemaAwareMockLlm;
    let request = json!({ "prompt": "hi", "output_parser": { "schema": null } });
    let out = llm.complete(request.clone(), None).await.expect("complete");
    assert_eq!(out["completion"], request);
}

#[test]
fn placeholder_for_schema_falls_back_to_type_without_properties() {
    assert_eq!(
        placeholder_for_schema(&json!({ "type": "array" })),
        json!([])
    );
    assert_eq!(
        placeholder_for_schema(&json!({ "type": "string" })),
        json!("")
    );
}

#[test]
fn placeholder_for_type_covers_every_json_schema_type() {
    assert_eq!(
        placeholder_for_type(&json!({ "type": "string" })),
        json!("")
    );
    assert_eq!(placeholder_for_type(&json!({ "type": "number" })), json!(0));
    assert_eq!(
        placeholder_for_type(&json!({ "type": "integer" })),
        json!(0)
    );
    assert_eq!(
        placeholder_for_type(&json!({ "type": "boolean" })),
        json!(false)
    );
    assert_eq!(
        placeholder_for_type(&json!({ "type": "object" })),
        json!({})
    );
    assert_eq!(placeholder_for_type(&json!({ "type": "array" })), json!([]));
    assert_eq!(placeholder_for_type(&json!({})), Value::Null);
}

#[test]
fn placeholder_for_type_prefers_the_first_enum_value_over_the_generic_type() {
    // A generic type placeholder (`""`) is essentially never one of an
    // enum's allowed values, so it must never be used when `enum` is set.
    assert_eq!(
        placeholder_for_type(&json!({ "type": "string", "enum": ["urgent", "normal"] })),
        json!("urgent")
    );
    // The first enum value wins even when its JSON type doesn't match
    // `type` (schema authors sometimes skip `type` entirely with `enum`).
    assert_eq!(
        placeholder_for_type(&json!({ "enum": [1, 2, 3] })),
        json!(1)
    );
}

#[test]
fn placeholder_for_type_ignores_an_empty_enum() {
    // An empty `enum` array has no first value to prefer — fall back to
    // the type-only placeholder rather than panicking or returning null.
    assert_eq!(
        placeholder_for_type(&json!({ "type": "string", "enum": [] })),
        json!("")
    );
}
