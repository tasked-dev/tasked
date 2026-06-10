//! Authentication middleware and the security helpers it relies on.

use axum::{
    Json, Router,
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::IntoResponse,
};

/// Returns true if `host` refers to a loopback address. Unresolvable
/// hostnames are conservatively treated as non-loopback.
pub(crate) fn is_loopback_host(host: &str) -> bool {
    if host == "localhost" {
        return true;
    }
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

/// Constant-time byte comparison to prevent timing attacks on API key validation.
/// Uses `subtle::ConstantTimeEq` which does not short-circuit on length mismatch.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

pub(crate) fn add_auth_layer(app: Router, auth_mode: &str, api_key: Option<&str>) -> Router {
    match auth_mode {
        "api-key" => {
            let key = match api_key {
                Some(k) => k.to_string(),
                None => {
                    eprintln!(
                        "fatal: --api-key (or TASKED_API_KEY) is required when --auth-mode=api-key"
                    );
                    std::process::exit(1);
                }
            };
            app.layer(axum::middleware::from_fn(
                move |req: Request<Body>, next: Next| {
                    let key = key.clone();
                    async move {
                        // Exempt liveness and metrics probes from API-key auth so
                        // load balancers and scrapers can reach them without
                        // credentials. Neither endpoint exposes flow data.
                        let path = req.uri().path();
                        if path == "/healthz" || path == "/metrics" {
                            return next.run(req).await;
                        }
                        let auth_header = req.headers().get("authorization");
                        let expected = format!("Bearer {key}");
                        match auth_header.and_then(|v| v.to_str().ok()) {
                            Some(val) if constant_time_eq(val.as_bytes(), expected.as_bytes()) => {
                                next.run(req).await
                            }
                            _ => {
                                let body = Json(serde_json::json!({
                                    "error": "unauthorized",
                                    "message": "Invalid or missing API key"
                                }));
                                (StatusCode::UNAUTHORIZED, body).into_response()
                            }
                        }
                    }
                },
            ))
        }
        "none" => app,
        other => {
            eprintln!("fatal: unrecognized auth mode '{other}'. Valid modes: none, api-key");
            std::process::exit(1);
        }
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_host_detection() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("127.0.0.2"));
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("[::1]"));

        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("::"));
        assert!(!is_loopback_host("192.168.1.10"));
        assert!(
            !is_loopback_host("example.com"),
            "unknown hostnames are treated as non-loopback"
        );
    }

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secres"));
        assert!(!constant_time_eq(b"secret", b"secret2"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }
}
