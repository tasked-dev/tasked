//! OAuth2 token refresh and caching for integration executors.
//!
//! Handles the OAuth2 refresh token flow: exchanges a refresh token for a
//! short-lived access token, caches it, and automatically refreshes when
//! expired. Tokens persist to SQLite across server restarts.

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::{debug, warn};

/// Cached OAuth2 access token.
#[derive(Debug, Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Option<DateTime<Utc>>,
}

impl CachedToken {
    /// Check if this token is still valid (with a 60-second buffer).
    fn is_valid(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => Utc::now() < expires_at - chrono::Duration::seconds(60),
            None => true, // No expiry = always valid (shouldn't happen in practice)
        }
    }
}

/// Token cache with optional SQLite persistence.
///
/// Shared across all integration executors via `Arc<TokenCache>`.
pub struct TokenCache {
    memory: Mutex<HashMap<String, CachedToken>>,
    db_path: Option<PathBuf>,
}

impl TokenCache {
    /// Create an in-memory-only token cache (no persistence).
    pub fn in_memory() -> Self {
        Self {
            memory: Mutex::new(HashMap::new()),
            db_path: None,
        }
    }

    /// Create a token cache with SQLite persistence.
    pub fn with_persistence(db_path: &Path) -> Self {
        let cache = Self {
            memory: Mutex::new(HashMap::new()),
            db_path: Some(db_path.to_path_buf()),
        };
        // Initialize the SQLite table
        if let Err(e) = cache.init_db() {
            warn!(error = %e, "failed to initialize token cache database, falling back to memory-only");
        } else {
            // Load existing tokens from disk into memory
            if let Err(e) = cache.load_from_db() {
                warn!(error = %e, "failed to load cached tokens from database");
            }
        }
        cache
    }

    /// Get a cached token, checking memory first, then SQLite.
    pub fn get_token(&self, key: &str) -> Option<String> {
        let cache = self.memory.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(token) = cache.get(key)
            && token.is_valid()
        {
            return Some(token.access_token.clone());
        }
        None
    }

    /// Store a token in memory and optionally persist to SQLite.
    pub fn set_token(&self, key: &str, access_token: &str, expires_in_secs: Option<u64>) {
        let expires_at =
            expires_in_secs.map(|secs| Utc::now() + chrono::Duration::seconds(secs as i64));

        let token = CachedToken {
            access_token: access_token.to_string(),
            expires_at,
        };

        self.memory
            .lock()
            .unwrap()
            .insert(key.to_string(), token.clone());

        // Persist to SQLite
        if self.db_path.is_some()
            && let Err(e) = self.persist_token(key, &token)
        {
            warn!(error = %e, key, "failed to persist token to database");
        }
    }

    #[cfg(feature = "sqlite")]
    fn init_db(&self) -> Result<(), String> {
        let path = self.db_path.as_ref().ok_or("no db path")?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create dir: {e}"))?;
        }
        let conn = rusqlite::Connection::open(path).map_err(|e| format!("open db: {e}"))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS oauth_tokens (
                 cache_key TEXT PRIMARY KEY,
                 access_token TEXT NOT NULL,
                 expires_at TEXT,
                 stored_at TEXT NOT NULL
             )",
        )
        .map_err(|e| format!("create table: {e}"))?;
        Ok(())
    }

    #[cfg(not(feature = "sqlite"))]
    fn init_db(&self) -> Result<(), String> {
        Ok(()) // No-op without sqlite feature
    }

    #[cfg(feature = "sqlite")]
    fn load_from_db(&self) -> Result<(), String> {
        let path = self.db_path.as_ref().ok_or("no db path")?;
        let conn = rusqlite::Connection::open(path).map_err(|e| format!("open db: {e}"))?;
        let mut stmt = conn
            .prepare("SELECT cache_key, access_token, expires_at FROM oauth_tokens")
            .map_err(|e| format!("prepare: {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                let key: String = row.get(0)?;
                let access_token: String = row.get(1)?;
                let expires_at_str: Option<String> = row.get(2)?;
                let expires_at = expires_at_str.and_then(|s| s.parse::<DateTime<Utc>>().ok());
                Ok((key, access_token, expires_at))
            })
            .map_err(|e| format!("query: {e}"))?;

        let mut cache = self.memory.lock().unwrap_or_else(|e| e.into_inner());
        let mut loaded = 0;
        for row in rows.flatten() {
            let (key, access_token, expires_at) = row;
            let token = CachedToken {
                access_token,
                expires_at,
            };
            if token.is_valid() {
                cache.insert(key, token);
                loaded += 1;
            }
        }
        if loaded > 0 {
            debug!(loaded, "loaded cached OAuth2 tokens from database");
        }
        Ok(())
    }

    #[cfg(not(feature = "sqlite"))]
    fn load_from_db(&self) -> Result<(), String> {
        Ok(())
    }

    #[cfg(feature = "sqlite")]
    fn persist_token(&self, key: &str, token: &CachedToken) -> Result<(), String> {
        let path = self.db_path.as_ref().ok_or("no db path")?;
        let conn = rusqlite::Connection::open(path).map_err(|e| format!("open db: {e}"))?;
        let expires_at_str = token.expires_at.map(|t| t.to_rfc3339());
        let stored_at = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT OR REPLACE INTO oauth_tokens (cache_key, access_token, expires_at, stored_at)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![key, token.access_token, expires_at_str, stored_at],
        )
        .map_err(|e| format!("insert: {e}"))?;
        Ok(())
    }

    #[cfg(not(feature = "sqlite"))]
    fn persist_token(&self, _key: &str, _token: &CachedToken) -> Result<(), String> {
        Ok(())
    }
}

/// Build a cache key from integration name and credential value.
///
/// Hashes the credential with SHA-256 to avoid storing secrets in the key.
/// SHA-256 is stable across Rust versions and platforms, unlike `DefaultHasher`.
pub fn cache_key(integration_name: &str, credential_json: &str) -> String {
    let hash = Sha256::digest(credential_json.as_bytes());
    let hex = hash
        .iter()
        .fold(String::with_capacity(64), |mut acc, byte| {
            use std::fmt::Write;
            let _ = write!(acc, "{byte:02x}");
            acc
        });
    format!("{integration_name}:{hex}")
}

/// OAuth2 token response from the authorization server.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    token_type: Option<String>,
}

/// Refresh an OAuth2 access token using the refresh token grant.
pub async fn refresh_token(
    client: &Client,
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
    scopes: Option<&str>,
) -> Result<(String, Option<u64>), String> {
    // Validate token_url against SSRF policy before making request
    crate::url_policy::validate_url(token_url)
        .map_err(|reason| format!("SSRF blocked (OAuth2 token_url): {reason}"))?;

    let mut params = vec![
        ("grant_type", "refresh_token"),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("refresh_token", refresh_token),
    ];
    if let Some(scopes) = scopes {
        params.push(("scope", scopes));
    }

    debug!(token_url, "refreshing OAuth2 access token");

    let resp = client
        .post(token_url)
        .form(&params)
        .send()
        .await
        .map_err(|e| format!("OAuth2 token request failed: {e}"))?;

    let status = resp.status().as_u16();
    let body = crate::executor::read_response_body(resp)
        .await
        .map_err(|e| format!("OAuth2 token response error: {e}"))?;

    if !(200..300).contains(&status) {
        return Err(format!(
            "OAuth2 token endpoint returned HTTP {status}: {body}"
        ));
    }

    let token_resp: TokenResponse = serde_json::from_str(&body)
        .map_err(|e| format!("failed to parse OAuth2 token response: {e}"))?;

    debug!(
        token_type = ?token_resp.token_type,
        expires_in = ?token_resp.expires_in,
        "OAuth2 token refreshed"
    );

    Ok((token_resp.access_token, token_resp.expires_in))
}
