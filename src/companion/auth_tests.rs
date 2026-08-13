use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

const EXTENSION_ID: &str = "abcdefghijklmnopabcdefghijklmnop";
const SECRET: &str = "0123456789abcdef0123456789abcdef";

fn authenticator() -> Authenticator {
    Authenticator::new(EXTENSION_ID, PairingSecret::parse(SECRET).unwrap()).unwrap()
}

#[test]
fn authenticates_only_exact_origin_protocol_and_secret() {
    let protocols = [
        PROTOCOL_SUBPROTOCOL,
        "tinyflows.auth.0123456789abcdef0123456789abcdef",
    ];
    let result = authenticator().authenticate(&WebSocketHandshake {
        origin: "chrome-extension://abcdefghijklmnopabcdefghijklmnop",
        subprotocols: &protocols,
    });
    assert_eq!(result.unwrap().negotiated_subprotocol, PROTOCOL_SUBPROTOCOL);
}

#[test]
fn rejects_lookalike_origin_and_url_style_auth() {
    let protocols = [PROTOCOL_SUBPROTOCOL];
    assert_eq!(
        authenticator()
            .authenticate(&WebSocketHandshake {
                origin: "chrome-extension://abcdefghijklmnopabcdefghijklmnop.example",
                subprotocols: &protocols,
            })
            .unwrap_err(),
        AuthError::OriginMismatch
    );
    assert_eq!(
        authenticator()
            .authenticate(&WebSocketHandshake {
                origin: "chrome-extension://abcdefghijklmnopabcdefghijklmnop",
                subprotocols: &protocols,
            })
            .unwrap_err(),
        AuthError::MissingAuthentication
    );
}

#[test]
fn rejects_wrong_protocol_secret_and_ambiguous_credentials() {
    let origin = "chrome-extension://abcdefghijklmnopabcdefghijklmnop";
    let wrong_protocol = [
        "tinyflows.v2",
        "tinyflows.auth.0123456789abcdef0123456789abcdef",
    ];
    assert_eq!(
        authenticator()
            .authenticate(&WebSocketHandshake {
                origin,
                subprotocols: &wrong_protocol,
            })
            .unwrap_err(),
        AuthError::ProtocolMismatch
    );

    let wrong_secret = [
        PROTOCOL_SUBPROTOCOL,
        "tinyflows.auth.ffffffffffffffffffffffffffffffff",
    ];
    assert_eq!(
        authenticator()
            .authenticate(&WebSocketHandshake {
                origin,
                subprotocols: &wrong_secret,
            })
            .unwrap_err(),
        AuthError::InvalidAuthentication
    );

    let ambiguous = [
        PROTOCOL_SUBPROTOCOL,
        "tinyflows.auth.0123456789abcdef0123456789abcdef",
        "tinyflows.auth.ffffffffffffffffffffffffffffffff",
    ];
    assert_eq!(
        authenticator()
            .authenticate(&WebSocketHandshake {
                origin,
                subprotocols: &ambiguous,
            })
            .unwrap_err(),
        AuthError::AmbiguousAuthentication
    );
}

#[test]
fn secret_store_round_trips_and_rotates() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!("tinyflows-secret-{unique}"));
    let store = SecretStore::new(directory.join("pairing-secret"));
    let first = store.load_or_create().unwrap();
    assert_eq!(store.load().unwrap(), first);
    let second = store.rotate().unwrap();
    assert_ne!(first, second);
    assert_eq!(store.load().unwrap(), second);
    fs::remove_dir_all(directory).unwrap();
}
