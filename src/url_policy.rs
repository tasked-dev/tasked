//! URL validation to prevent SSRF (Server-Side Request Forgery).
//!
//! Rejects URLs that target private, loopback, or link-local addresses
//! to prevent attackers from probing internal networks or accessing
//! cloud metadata endpoints (e.g., 169.254.169.254).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;

/// Build a [`reqwest::Client`] with SSRF protection at the DNS resolution layer.
///
/// Uses a custom DNS resolver (SsrfSafeResolver) that validates every resolved
/// IP address **at connection time**, preventing DNS rebinding attacks where a
/// hostname resolves to a public IP during validation but to a private IP when
/// the actual TCP connection is made.
///
/// Also includes a redirect policy that re-validates each redirect target as
/// defense-in-depth.
///
/// All HTTP-capable code paths should use this instead of `reqwest::Client::new()`.
#[cfg(feature = "http")]
pub fn ssrf_safe_client() -> reqwest::Client {
    reqwest::Client::builder()
        .dns_resolver(Arc::new(SsrfSafeResolver))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            let url = attempt.url().as_str();
            match validate_url(url) {
                Ok(()) => attempt.follow(),
                Err(reason) => attempt.error(SsrfRedirectError(reason)),
            }
        }))
        .build()
        .expect("failed to build reqwest client")
}

/// Custom DNS resolver that rejects private/reserved IP addresses at resolution
/// time, closing the DNS rebinding TOCTOU window.
#[cfg(feature = "http")]
struct SsrfSafeResolver;

#[cfg(feature = "http")]
impl reqwest::dns::Resolve for SsrfSafeResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();

        // In test builds, always allow loopback so tests can use localhost mock
        // servers. The validate_url() defense-in-depth layer still has per-test
        // control via the ALLOW_LOOPBACK atomic.
        #[cfg(any(test, feature = "test-utils"))]
        let allow_loopback = true;
        #[cfg(not(any(test, feature = "test-utils")))]
        let allow_loopback = false;

        Box::pin(async move {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host(format!("{host}:0"))
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("DNS resolution failed for '{host}': {e}").into()
                })?
                .collect();

            if addrs.is_empty() {
                return Err(format!("DNS resolution for '{host}' returned no addresses").into());
            }

            for addr in &addrs {
                let blocked = if allow_loopback {
                    is_private_or_reserved(addr.ip()) && !addr.ip().is_loopback()
                } else {
                    is_private_or_reserved(addr.ip())
                };
                if blocked {
                    return Err(format!(
                        "DNS resolved '{host}' to private/reserved address {} — blocked (SSRF protection)",
                        addr.ip()
                    )
                    .into());
                }
            }

            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Error type returned when a redirect targets a private/reserved address.
#[cfg(feature = "http")]
#[derive(Debug)]
struct SsrfRedirectError(String);

#[cfg(feature = "http")]
impl std::fmt::Display for SsrfRedirectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SSRF blocked redirect: {}", self.0)
    }
}

#[cfg(feature = "http")]
impl std::error::Error for SsrfRedirectError {}

#[cfg(any(test, feature = "test-utils"))]
std::thread_local! {
    /// Per-thread loopback bypass flag for test builds.
    static ALLOW_LOOPBACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Enable loopback addresses (127.0.0.0/8) for the current thread.
/// Only available in integration-test binaries that set `TASKED_TEST=1`.
#[cfg(all(not(test), feature = "test-utils"))]
pub fn allow_loopback_for_tests(allow: bool) {
    if std::env::var("TASKED_TEST").as_deref() != Ok("1") {
        panic!(
            "allow_loopback_for_tests() called outside of a test context. \
             Set TASKED_TEST=1 to confirm this is an integration test."
        );
    }
    ALLOW_LOOPBACK.with(|cell| cell.set(allow));
}

/// Check a URL's host without DNS: returns `Ok(Some(()))` if the host is an
/// IP literal that passed validation, `Ok(None)` if it is a hostname that
/// still needs resolving, or `Err` if it is a blocked IP literal.
fn validate_url_pre_dns(url: &str) -> Result<Option<&str>, String> {
    let host = extract_host(url).ok_or_else(|| format!("cannot parse host from URL: {url}"))?;

    // If the host is a raw IP, check it directly
    if let Ok(ip) = host.parse::<IpAddr>() {
        check_ip(ip)?;
        return Ok(None);
    }

    // Handle IPv6 bracket notation: [::1]
    if let Some(inner) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']'))
        && let Ok(ip) = inner.parse::<Ipv6Addr>()
    {
        check_ip(IpAddr::V6(ip))?;
        return Ok(None);
    }

    Ok(Some(host))
}

/// Validate that a URL does not target a private or reserved IP address.
///
/// Resolves the hostname via DNS and checks all resolved addresses.
/// Returns `Ok(())` if the URL is safe to request, or `Err` with a reason.
///
/// Note: hostname resolution here is blocking — prefer
/// [`validate_url_async`] from async contexts. This sync version exists for
/// the reqwest redirect-policy callback, which cannot await.
pub fn validate_url(url: &str) -> Result<(), String> {
    let Some(host) = validate_url_pre_dns(url)? else {
        return Ok(());
    };

    // Resolve hostname and check all addresses
    let port = extract_port(url).unwrap_or(80);
    let addr_str = format!("{host}:{port}");
    let addrs = addr_str
        .to_socket_addrs()
        .map_err(|e| format!("DNS resolution failed for '{host}': {e}"))?;

    for addr in addrs {
        check_ip(addr.ip())?;
    }

    Ok(())
}

/// Async variant of [`validate_url`] using tokio's resolver, so a slow DNS
/// server can't pin an async runtime worker thread.
pub async fn validate_url_async(url: &str) -> Result<(), String> {
    let Some(host) = validate_url_pre_dns(url)? else {
        return Ok(());
    };

    let port = extract_port(url).unwrap_or(80);
    let addrs = tokio::net::lookup_host(format!("{host}:{port}"))
        .await
        .map_err(|e| format!("DNS resolution failed for '{host}': {e}"))?;

    for addr in addrs {
        check_ip(addr.ip())?;
    }

    Ok(())
}

/// Check whether an IP address is private/reserved.
fn check_ip(ip: IpAddr) -> Result<(), String> {
    if is_private_or_reserved(ip) {
        return Err(format!(
            "request to private/reserved address {ip} is blocked"
        ));
    }
    Ok(())
}

/// Returns true if the IP is in a private, loopback, link-local, or reserved range.
fn is_private_or_reserved(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_private_v4(v4),
        IpAddr::V6(v6) => is_private_v6(v6),
    }
}

fn is_private_v4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();

    // In test builds, always allow loopback so tests can use localhost mock servers.
    #[cfg(any(test, feature = "test-utils"))]
    if octets[0] == 127 {
        return false;
    }

    // 0.0.0.0/8 — unspecified
    octets[0] == 0
    // 10.0.0.0/8 — private
    || octets[0] == 10
    // 100.64.0.0/10 — carrier-grade NAT
    || (octets[0] == 100 && (64..=127).contains(&octets[1]))
    // 127.0.0.0/8 — loopback
    || octets[0] == 127
    // 169.254.0.0/16 — link-local (cloud metadata)
    || (octets[0] == 169 && octets[1] == 254)
    // 172.16.0.0/12 — private
    || (octets[0] == 172 && (16..=31).contains(&octets[1]))
    // 192.0.0.0/24 — IETF protocol assignments
    || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
    // 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24 — documentation
    || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
    || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
    || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
    // 192.168.0.0/16 — private
    || (octets[0] == 192 && octets[1] == 168)
    // 198.18.0.0/15 — benchmarking
    || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
    // 224.0.0.0/4 — multicast; 240.0.0.0/4 — reserved (incl. broadcast)
    || octets[0] >= 224
}

fn is_private_v6(ip: Ipv6Addr) -> bool {
    // In test builds, always allow loopback so tests can use localhost mock servers.
    #[cfg(any(test, feature = "test-utils"))]
    if ip == Ipv6Addr::LOCALHOST {
        return false;
    }

    // ::1 — loopback
    ip == Ipv6Addr::LOCALHOST
    // :: — unspecified
    || ip == Ipv6Addr::UNSPECIFIED
    // fc00::/7 — unique local
    || (ip.segments()[0] & 0xfe00) == 0xfc00
    // fe80::/10 — link-local
    || (ip.segments()[0] & 0xffc0) == 0xfe80
    // ff00::/8 — multicast
    || (ip.segments()[0] & 0xff00) == 0xff00
    // 2001:db8::/32 — documentation
    || (ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8)
    // ::ffff:0:0/96 — IPv4-mapped (check the embedded v4)
    || ip.to_ipv4_mapped().is_some_and(is_private_v4)
}

/// Returns `true` if the URL uses plain HTTP and the host is **not** a loopback
/// address (127.0.0.0/8 or ::1).  Callers should emit a warning when this
/// returns `true` because bearer tokens or other credentials would travel
/// over an unencrypted connection.
///
/// Returns `false` (safe / no warning needed) when:
/// - the scheme is `https`,
/// - the host is a loopback address (local development), or
/// - the URL cannot be parsed (other validation will catch that).
pub fn is_insecure_non_loopback(url: &str) -> bool {
    let scheme = url
        .find("://")
        .map(|i| &url[..i])
        .unwrap_or("")
        .to_ascii_lowercase();

    // Only flag plain HTTP — other schemes (https, unix, etc.) are fine or
    // will be rejected elsewhere.
    if scheme != "http" {
        return false;
    }

    let host = match extract_host(url) {
        Some(h) => h,
        None => return false, // unparseable → let other validation handle it
    };

    // Strip IPv6 brackets for parsing: "[::1]" → "::1"
    let bare = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);

    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return !ip.is_loopback();
    }

    // Hostname "localhost" is conventionally loopback — don't warn.
    if bare.eq_ignore_ascii_case("localhost") {
        return false;
    }

    // Any other hostname over plain HTTP is insecure.
    true
}

/// Extract the host from a URL string (no external URL crate needed).
fn extract_host(url: &str) -> Option<&str> {
    // Skip scheme: "http://", "https://"
    let after_scheme = url.find("://").map(|i| &url[i + 3..])?;

    // Strip userinfo: "user:pass@host" → "host"
    let after_userinfo = after_scheme
        .find('@')
        .map(|i| &after_scheme[i + 1..])
        .unwrap_or(after_scheme);

    // Take until path, query, or fragment
    let host_port = after_userinfo
        .split_once('/')
        .map(|(h, _)| h)
        .unwrap_or(after_userinfo);
    let host_port = host_port
        .split_once('?')
        .map(|(h, _)| h)
        .unwrap_or(host_port);
    let host_port = host_port
        .split_once('#')
        .map(|(h, _)| h)
        .unwrap_or(host_port);

    // Strip port: "host:8080" → "host" (but not for IPv6 bracket notation)
    let host = if host_port.starts_with('[') {
        // IPv6: [::1]:8080
        host_port
            .find(']')
            .map(|i| &host_port[..=i])
            .unwrap_or(host_port)
    } else {
        host_port
            .rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or(host_port)
    };

    if host.is_empty() { None } else { Some(host) }
}

/// Extract the port from a URL string, if present.
fn extract_port(url: &str) -> Option<u16> {
    let after_scheme = url.find("://").map(|i| &url[i + 3..])?;
    let host_port = after_scheme.split('/').next()?;

    if host_port.starts_with('[') {
        // IPv6: [::1]:8080
        let after_bracket = host_port.find(']').map(|i| &host_port[i + 1..])?;
        after_bracket.strip_prefix(':').and_then(|p| p.parse().ok())
    } else {
        host_port.rsplit_once(':').and_then(|(_, p)| p.parse().ok())
    }
}
