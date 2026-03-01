//! I/O capability trait for explicit capability-based I/O access.
//!
//! The [`IoCap`] trait defines the capability boundary for I/O operations.
//! Tasks can only perform I/O if they have access to an `IoCap` implementation.
//!
//! # Design Rationale
//!
//! Asupersync uses explicit capability security - no ambient authority. I/O operations
//! are only available when the runtime provides an `IoCap` implementation:
//!
//! - Production runtime provides a real I/O capability backed by the reactor
//! - Lab runtime provides a virtual I/O capability for deterministic testing
//! - Tests can verify that code correctly handles "no I/O" scenarios
//!
//! # Two-Phase I/O Model
//!
//! I/O operations in Asupersync follow a two-phase commit model:
//!
//! 1. **Submit**: Create an I/O operation (returns a handle/obligation)
//! 2. **Complete**: Wait for completion or cancel
//!
//! This model allows for proper cancellation tracking and budget accounting.

use std::fmt::Debug;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

/// Capability surface advertised by an [`IoCap`] implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct IoCapabilities {
    /// Supports real file descriptor backed operations.
    pub file_ops: bool,
    /// Supports real socket operations.
    pub network_ops: bool,
    /// Supports timer-backed I/O wakeups.
    pub timer_integration: bool,
    /// Provides deterministic virtual I/O semantics.
    pub deterministic: bool,
}

impl IoCapabilities {
    /// Capability descriptor for virtual deterministic I/O.
    pub const LAB: Self = Self {
        file_ops: false,
        network_ops: false,
        timer_integration: true,
        deterministic: true,
    };

    /// Capability descriptor for browser environment I/O.
    ///
    /// Browser I/O supports network operations (fetch, WebSocket) and timer
    /// integration (setTimeout/setInterval bridged to the runtime), but does
    /// not support file descriptor operations or provide deterministic semantics.
    pub const BROWSER: Self = Self {
        file_ops: false,
        network_ops: true,
        timer_integration: true,
        deterministic: false,
    };
}

/// Snapshot of I/O operation counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IoStats {
    /// Number of operations submitted through the capability.
    pub submitted: u64,
    /// Number of operations completed through the capability.
    pub completed: u64,
}

/// HTTP method allowlist for browser fetch capability checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FetchMethod {
    /// HTTP GET.
    Get,
    /// HTTP POST.
    Post,
    /// HTTP PUT.
    Put,
    /// HTTP PATCH.
    Patch,
    /// HTTP DELETE.
    Delete,
    /// HTTP HEAD.
    Head,
    /// HTTP OPTIONS.
    Options,
}

/// Request envelope used for explicit fetch authority checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    /// HTTP method.
    pub method: FetchMethod,
    /// Absolute URL.
    pub url: String,
    /// Request headers.
    pub headers: Vec<(String, String)>,
    /// Whether credentials are requested.
    pub credentials: bool,
}

impl FetchRequest {
    /// Creates a new request envelope.
    #[must_use]
    pub fn new(method: FetchMethod, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: Vec::new(),
            credentials: false,
        }
    }

    /// Adds a request header.
    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Enables credentialed fetch.
    #[must_use]
    pub fn with_credentials(mut self) -> Self {
        self.credentials = true;
        self
    }

    fn origin(&self) -> Option<&str> {
        let scheme_end = self.url.find("://")?;
        if scheme_end == 0 {
            return None;
        }
        let rest = &self.url[scheme_end + 3..];
        if rest.is_empty() {
            return None;
        }
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        if authority_end == 0 {
            return None;
        }
        Some(&self.url[..scheme_end + 3 + authority_end])
    }
}

/// Deterministic policy errors for fetch capability checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchPolicyError {
    /// URL did not contain a valid origin.
    InvalidUrl(String),
    /// Origin is outside the explicit allowlist.
    OriginDenied(String),
    /// Method is outside the explicit allowlist.
    MethodDenied(FetchMethod),
    /// Credentialed fetch is not permitted by policy.
    CredentialsDenied,
    /// Header count exceeds policy.
    TooManyHeaders {
        /// Header count found in the request.
        count: usize,
        /// Maximum allowed header count.
        limit: usize,
    },
}

impl std::fmt::Display for FetchPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUrl(url) => write!(f, "invalid fetch URL: {url}"),
            Self::OriginDenied(origin) => write!(f, "fetch origin denied by policy: {origin}"),
            Self::MethodDenied(method) => write!(f, "fetch method denied by policy: {method:?}"),
            Self::CredentialsDenied => write!(f, "credentialed fetch denied by policy"),
            Self::TooManyHeaders { count, limit } => {
                write!(f, "header count {count} exceeds fetch policy limit {limit}")
            }
        }
    }
}

impl std::error::Error for FetchPolicyError {}

/// Explicit authority boundaries for browser fetch operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchAuthority {
    /// Allowed origins (`scheme://host[:port]`). Empty means no origin authority.
    pub allowed_origins: Vec<String>,
    /// Allowed HTTP methods. Empty means no method authority.
    pub allowed_methods: Vec<FetchMethod>,
    /// Whether credentialed requests are permitted.
    pub allow_credentials: bool,
    /// Maximum allowed header count.
    pub max_header_count: usize,
}

impl Default for FetchAuthority {
    fn default() -> Self {
        Self::deny_all()
    }
}

impl FetchAuthority {
    /// Creates an authority with no grants (default-deny posture).
    #[must_use]
    pub fn deny_all() -> Self {
        Self {
            allowed_origins: Vec::new(),
            allowed_methods: Vec::new(),
            allow_credentials: false,
            max_header_count: 0,
        }
    }

    /// Grants authority for a specific origin.
    #[must_use]
    pub fn grant_origin(mut self, origin: impl Into<String>) -> Self {
        let origin = origin.into();
        if !origin.is_empty()
            && !self
                .allowed_origins
                .iter()
                .any(|candidate| candidate == &origin)
        {
            self.allowed_origins.push(origin);
        }
        self
    }

    /// Grants authority for a specific HTTP method.
    #[must_use]
    pub fn grant_method(mut self, method: FetchMethod) -> Self {
        if !self.allowed_methods.contains(&method) {
            self.allowed_methods.push(method);
        }
        self
    }

    /// Sets the maximum request header count.
    #[must_use]
    pub fn with_max_header_count(mut self, max_header_count: usize) -> Self {
        self.max_header_count = max_header_count;
        self
    }

    /// Enables credentialed fetch authority.
    #[must_use]
    pub fn with_credentials_allowed(mut self) -> Self {
        self.allow_credentials = true;
        self
    }

    /// Validates a request against authority boundaries.
    pub fn authorize(&self, request: &FetchRequest) -> Result<(), FetchPolicyError> {
        let origin = request
            .origin()
            .ok_or_else(|| FetchPolicyError::InvalidUrl(request.url.clone()))?;

        let origin_allowed = self
            .allowed_origins
            .iter()
            .any(|candidate| candidate == "*" || candidate == origin);
        if !origin_allowed {
            return Err(FetchPolicyError::OriginDenied(origin.to_owned()));
        }

        if !self.allowed_methods.contains(&request.method) {
            return Err(FetchPolicyError::MethodDenied(request.method));
        }

        if request.credentials && !self.allow_credentials {
            return Err(FetchPolicyError::CredentialsDenied);
        }

        if request.headers.len() > self.max_header_count {
            return Err(FetchPolicyError::TooManyHeaders {
                count: request.headers.len(),
                limit: self.max_header_count,
            });
        }

        Ok(())
    }
}

/// Timeout policy for browser fetch operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchTimeoutPolicy {
    /// End-to-end timeout for request lifecycle.
    pub request_timeout_ms: u64,
    /// Maximum wait for first response byte.
    pub first_byte_timeout_ms: u64,
    /// Maximum idle gap between streamed response chunks.
    pub between_chunks_timeout_ms: u64,
}

impl Default for FetchTimeoutPolicy {
    fn default() -> Self {
        Self {
            request_timeout_ms: 30_000,
            first_byte_timeout_ms: 10_000,
            between_chunks_timeout_ms: 5_000,
        }
    }
}

/// Streaming and header/body bounds for browser fetch operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchStreamPolicy {
    /// Maximum serialized request body size.
    pub max_request_body_bytes: usize,
    /// Maximum streamed response body size.
    pub max_response_body_bytes: usize,
    /// Maximum aggregate response header bytes.
    pub max_response_header_bytes: usize,
}

impl Default for FetchStreamPolicy {
    fn default() -> Self {
        Self {
            max_request_body_bytes: 4 * 1024 * 1024,
            max_response_body_bytes: 16 * 1024 * 1024,
            max_response_header_bytes: 16 * 1024,
        }
    }
}

/// Cancellation contract for fetch adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchCancellationPolicy {
    /// Cancellation requires host abort signaling and drains partial body state.
    AbortSignalWithDrain,
    /// Cancellation requests cooperative stop without host-level abort.
    CooperativeOnly,
}

/// Fetch capability interface surfaced through [`IoCap`].
pub trait FetchIoCap: Send + Sync + Debug {
    /// Validates a request against explicit authority policy.
    fn authorize(&self, request: &FetchRequest) -> Result<(), FetchPolicyError>;

    /// Returns the timeout policy.
    fn timeout_policy(&self) -> FetchTimeoutPolicy;

    /// Returns streaming/header-body bounds.
    fn stream_policy(&self) -> FetchStreamPolicy;

    /// Returns cancellation semantics.
    fn cancellation_policy(&self) -> FetchCancellationPolicy;
}

/// Browser-oriented fetch adapter carrying explicit authority and policy.
#[derive(Debug, Clone)]
pub struct BrowserFetchIoCap {
    authority: FetchAuthority,
    timeout: FetchTimeoutPolicy,
    stream: FetchStreamPolicy,
    cancellation: FetchCancellationPolicy,
}

impl BrowserFetchIoCap {
    /// Creates a new browser fetch capability adapter.
    #[must_use]
    pub fn new(
        authority: FetchAuthority,
        timeout: FetchTimeoutPolicy,
        stream: FetchStreamPolicy,
        cancellation: FetchCancellationPolicy,
    ) -> Self {
        Self {
            authority,
            timeout,
            stream,
            cancellation,
        }
    }
}

/// Browser long-lived transport kind requiring explicit authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BrowserTransportKind {
    /// RFC 6455 WebSocket channel.
    WebSocket,
    /// WebTransport session (HTTPS-only in browsers).
    WebTransport,
}

/// Request envelope used for explicit transport authority checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserTransportRequest {
    /// Requested transport kind.
    pub kind: BrowserTransportKind,
    /// Absolute URL for the transport endpoint.
    pub url: String,
    /// Requested subprotocols (WebSocket only).
    pub subprotocols: Vec<String>,
    /// Reconnect attempt index (0 for initial connection).
    pub reconnect_attempt: u32,
}

impl BrowserTransportRequest {
    /// Creates a new transport request envelope.
    #[must_use]
    pub fn new(kind: BrowserTransportKind, url: impl Into<String>) -> Self {
        Self {
            kind,
            url: url.into(),
            subprotocols: Vec::new(),
            reconnect_attempt: 0,
        }
    }

    /// Adds a requested subprotocol.
    #[must_use]
    pub fn with_subprotocol(mut self, protocol: impl Into<String>) -> Self {
        self.subprotocols.push(protocol.into());
        self
    }

    /// Sets reconnect attempt metadata.
    #[must_use]
    pub fn with_reconnect_attempt(mut self, reconnect_attempt: u32) -> Self {
        self.reconnect_attempt = reconnect_attempt;
        self
    }
}

fn parse_browser_transport_url(url: &str) -> Option<(String, String, String)> {
    let scheme_end = url.find("://")?;
    if scheme_end == 0 {
        return None;
    }

    let scheme = url[..scheme_end].to_owned();
    let rest = &url[scheme_end + 3..];
    if rest.is_empty() {
        return None;
    }

    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    if authority_end == 0 {
        return None;
    }
    let authority = &rest[..authority_end];
    let origin = format!("{scheme}://{authority}");

    let host_authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if host_authority.is_empty() {
        return None;
    }

    let host = if let Some(rest) = host_authority.strip_prefix('[') {
        let closing = rest.find(']')?;
        rest[..closing].to_owned()
    } else {
        host_authority.split(':').next()?.to_owned()
    };

    if host.is_empty() {
        return None;
    }

    Some((scheme, origin, host))
}

/// Deterministic policy errors for browser transport capability checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowserTransportPolicyError {
    /// URL did not contain a valid scheme/authority.
    InvalidUrl(String),
    /// Origin is outside explicit allowlist.
    OriginDenied(String),
    /// Transport kind is outside explicit allowlist.
    KindDenied(BrowserTransportKind),
    /// Transport kind is unsupported in current browser context.
    UnsupportedKind(BrowserTransportKind),
    /// URL scheme is not valid for requested transport/security policy.
    InsecureScheme {
        /// Requested transport kind.
        kind: BrowserTransportKind,
        /// Requested scheme.
        scheme: String,
    },
    /// Requested subprotocol count exceeds policy.
    TooManySubprotocols {
        /// Subprotocol count found in request.
        count: usize,
        /// Maximum allowed subprotocol count.
        limit: usize,
    },
    /// Reconnect attempt exceeds configured policy.
    ReconnectAttemptExceeded {
        /// Requested reconnect attempt.
        attempt: u32,
        /// Maximum permitted reconnect attempt.
        max_attempts: u32,
    },
}

impl std::fmt::Display for BrowserTransportPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUrl(url) => write!(f, "invalid browser transport URL: {url}"),
            Self::OriginDenied(origin) => {
                write!(f, "browser transport origin denied by policy: {origin}")
            }
            Self::KindDenied(kind) => {
                write!(f, "browser transport kind denied by policy: {kind:?}")
            }
            Self::UnsupportedKind(kind) => {
                write!(
                    f,
                    "browser transport kind unsupported in this context: {kind:?}"
                )
            }
            Self::InsecureScheme { kind, scheme } => {
                write!(
                    f,
                    "scheme '{scheme}' is invalid for browser transport {kind:?}"
                )
            }
            Self::TooManySubprotocols { count, limit } => {
                write!(
                    f,
                    "subprotocol count {count} exceeds browser transport policy limit {limit}"
                )
            }
            Self::ReconnectAttemptExceeded {
                attempt,
                max_attempts,
            } => write!(
                f,
                "reconnect attempt {attempt} exceeds browser transport policy max {max_attempts}"
            ),
        }
    }
}

impl std::error::Error for BrowserTransportPolicyError {}

/// Explicit authority boundaries for browser transport operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserTransportAuthority {
    /// Allowed origins (`scheme://host[:port]`). Empty means no origin authority.
    pub allowed_origins: Vec<String>,
    /// Allowed transport kinds. Empty means no transport authority.
    pub allowed_kinds: Vec<BrowserTransportKind>,
    /// Maximum allowed subprotocol count per request.
    pub max_subprotocol_count: usize,
    /// Allows plain `ws://` when host is loopback/localhost only.
    pub allow_insecure_localhost_ws: bool,
}

impl Default for BrowserTransportAuthority {
    fn default() -> Self {
        Self::deny_all()
    }
}

impl BrowserTransportAuthority {
    /// Creates an authority with no grants (default-deny posture).
    #[must_use]
    pub fn deny_all() -> Self {
        Self {
            allowed_origins: Vec::new(),
            allowed_kinds: Vec::new(),
            max_subprotocol_count: 0,
            allow_insecure_localhost_ws: false,
        }
    }

    /// Grants authority for a specific origin.
    #[must_use]
    pub fn grant_origin(mut self, origin: impl Into<String>) -> Self {
        let origin = origin.into();
        if !origin.is_empty()
            && !self
                .allowed_origins
                .iter()
                .any(|candidate| candidate == &origin)
        {
            self.allowed_origins.push(origin);
        }
        self
    }

    /// Grants authority for a specific transport kind.
    #[must_use]
    pub fn grant_kind(mut self, kind: BrowserTransportKind) -> Self {
        if !self.allowed_kinds.contains(&kind) {
            self.allowed_kinds.push(kind);
        }
        self
    }

    /// Sets maximum allowed subprotocol count.
    #[must_use]
    pub fn with_max_subprotocol_count(mut self, max_subprotocol_count: usize) -> Self {
        self.max_subprotocol_count = max_subprotocol_count;
        self
    }

    /// Enables localhost-only insecure websocket (`ws://`) authority.
    #[must_use]
    pub fn with_localhost_insecure_ws(mut self) -> Self {
        self.allow_insecure_localhost_ws = true;
        self
    }
}

/// Browser support matrix for long-lived transport channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrowserTransportSupport {
    /// Browser context supports WebSocket.
    pub websocket: bool,
    /// Browser context supports WebTransport.
    pub webtransport: bool,
}

impl BrowserTransportSupport {
    /// No long-lived transport support in the current context.
    pub const NONE: Self = Self {
        websocket: false,
        webtransport: false,
    };

    /// WebSocket-only support.
    pub const WEBSOCKET_ONLY: Self = Self {
        websocket: true,
        webtransport: false,
    };

    /// WebSocket and WebTransport support.
    pub const FULL: Self = Self {
        websocket: true,
        webtransport: true,
    };

    fn supports(self, kind: BrowserTransportKind) -> bool {
        match kind {
            BrowserTransportKind::WebSocket => self.websocket,
            BrowserTransportKind::WebTransport => self.webtransport,
        }
    }
}

/// Reconnection policy for browser long-lived transport channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrowserTransportReconnectPolicy {
    /// Maximum reconnect attempts after initial connection.
    pub max_attempts: u32,
    /// Base delay for reconnect backoff.
    pub base_delay_ms: u64,
    /// Maximum reconnect backoff delay.
    pub max_delay_ms: u64,
    /// Deterministic jitter window (0 keeps strictly deterministic delay).
    pub jitter_ms: u64,
}

impl Default for BrowserTransportReconnectPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 250,
            max_delay_ms: 5_000,
            jitter_ms: 0,
        }
    }
}

/// Cancellation contract for browser long-lived transport adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserTransportCancellationPolicy {
    /// Send protocol close signal first, then host-abort if drain deadline expires.
    CloseThenAbort,
    /// Abort immediately on cancellation request.
    ImmediateAbort,
}

/// Transport capability interface surfaced through [`IoCap`].
pub trait TransportIoCap: Send + Sync + Debug {
    /// Validates a request against explicit authority and support policy.
    fn authorize(
        &self,
        request: &BrowserTransportRequest,
    ) -> Result<(), BrowserTransportPolicyError>;

    /// Returns browser transport support matrix.
    fn support(&self) -> BrowserTransportSupport;

    /// Returns cancellation semantics.
    fn cancellation_policy(&self) -> BrowserTransportCancellationPolicy;

    /// Returns reconnection semantics.
    fn reconnect_policy(&self) -> BrowserTransportReconnectPolicy;
}

/// Browser-oriented transport adapter carrying explicit authority and policy.
#[derive(Debug, Clone)]
pub struct BrowserTransportIoCap {
    authority: BrowserTransportAuthority,
    support: BrowserTransportSupport,
    cancellation: BrowserTransportCancellationPolicy,
    reconnect: BrowserTransportReconnectPolicy,
}

impl BrowserTransportIoCap {
    /// Creates a new browser transport capability adapter.
    #[must_use]
    pub fn new(
        authority: BrowserTransportAuthority,
        support: BrowserTransportSupport,
        cancellation: BrowserTransportCancellationPolicy,
        reconnect: BrowserTransportReconnectPolicy,
    ) -> Self {
        Self {
            authority,
            support,
            cancellation,
            reconnect,
        }
    }
}

/// Browser storage backend target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StorageBackend {
    /// IndexedDB durable key/value storage.
    IndexedDb,
    /// localStorage string key/value storage.
    LocalStorage,
}

/// Storage operations that require explicit authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageOperation {
    /// Load a value by key.
    Get,
    /// Persist or update a value by key.
    Set,
    /// Delete a single key.
    Delete,
    /// Enumerate keys for a namespace.
    ListKeys,
    /// Remove all keys in a namespace.
    ClearNamespace,
}

/// Request envelope used for explicit browser storage authority checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRequest {
    /// Target backend.
    pub backend: StorageBackend,
    /// Requested operation.
    pub operation: StorageOperation,
    /// Explicit namespace to avoid ambient global keys.
    pub namespace: String,
    /// Optional key for key-scoped operations.
    pub key: Option<String>,
    /// Value length for write-style operations.
    pub value_len: usize,
}

impl StorageRequest {
    /// Creates a new storage request.
    #[must_use]
    pub fn new(
        backend: StorageBackend,
        operation: StorageOperation,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            backend,
            operation,
            namespace: namespace.into(),
            key: None,
            value_len: 0,
        }
    }

    /// Adds a key to the request.
    #[must_use]
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Adds value byte-length metadata.
    #[must_use]
    pub fn with_value_len(mut self, value_len: usize) -> Self {
        self.value_len = value_len;
        self
    }

    /// Convenience constructor for `Get`.
    #[must_use]
    pub fn get(
        backend: StorageBackend,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self::new(backend, StorageOperation::Get, namespace).with_key(key)
    }

    /// Convenience constructor for `Set`.
    #[must_use]
    pub fn set(
        backend: StorageBackend,
        namespace: impl Into<String>,
        key: impl Into<String>,
        value_len: usize,
    ) -> Self {
        Self::new(backend, StorageOperation::Set, namespace)
            .with_key(key)
            .with_value_len(value_len)
    }

    /// Convenience constructor for `Delete`.
    #[must_use]
    pub fn delete(
        backend: StorageBackend,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self::new(backend, StorageOperation::Delete, namespace).with_key(key)
    }

    /// Convenience constructor for `ListKeys`.
    #[must_use]
    pub fn list_keys(backend: StorageBackend, namespace: impl Into<String>) -> Self {
        Self::new(backend, StorageOperation::ListKeys, namespace)
    }

    /// Convenience constructor for `ClearNamespace`.
    #[must_use]
    pub fn clear_namespace(backend: StorageBackend, namespace: impl Into<String>) -> Self {
        Self::new(backend, StorageOperation::ClearNamespace, namespace)
    }
}

/// Deterministic policy errors for browser storage capability checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoragePolicyError {
    /// Namespace shape was empty or invalid.
    InvalidNamespace(String),
    /// Requested backend is outside explicit authority.
    BackendDenied(StorageBackend),
    /// Namespace is outside explicit authority.
    NamespaceDenied(String),
    /// Operation is outside explicit authority.
    OperationDenied(StorageOperation),
    /// Key is required for this operation.
    MissingKey(StorageOperation),
    /// Key length exceeds policy.
    KeyTooLarge {
        /// Key length in bytes.
        len: usize,
        /// Maximum allowed key length.
        limit: usize,
    },
    /// Value size exceeds policy.
    ValueTooLarge {
        /// Value length in bytes.
        len: usize,
        /// Maximum allowed value length.
        limit: usize,
    },
    /// Namespace length exceeds policy.
    NamespaceTooLarge {
        /// Namespace length in bytes.
        len: usize,
        /// Maximum allowed namespace length.
        limit: usize,
    },
    /// Entry count would exceed policy.
    EntryCountExceeded {
        /// Projected entry count.
        projected: usize,
        /// Maximum allowed entries.
        limit: usize,
    },
    /// Aggregate storage usage would exceed policy.
    QuotaExceeded {
        /// Projected bytes after operation.
        projected_bytes: usize,
        /// Maximum allowed bytes.
        limit_bytes: usize,
    },
}

impl std::fmt::Display for StoragePolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNamespace(namespace) => {
                write!(f, "invalid storage namespace: {namespace}")
            }
            Self::BackendDenied(backend) => {
                write!(f, "storage backend denied by policy: {backend:?}")
            }
            Self::NamespaceDenied(namespace) => {
                write!(f, "storage namespace denied by policy: {namespace}")
            }
            Self::OperationDenied(operation) => {
                write!(f, "storage operation denied by policy: {operation:?}")
            }
            Self::MissingKey(operation) => {
                write!(f, "storage operation requires key: {operation:?}")
            }
            Self::KeyTooLarge { len, limit } => {
                write!(f, "storage key length {len} exceeds policy limit {limit}")
            }
            Self::ValueTooLarge { len, limit } => {
                write!(f, "storage value length {len} exceeds policy limit {limit}")
            }
            Self::NamespaceTooLarge { len, limit } => {
                write!(
                    f,
                    "storage namespace length {len} exceeds policy limit {limit}"
                )
            }
            Self::EntryCountExceeded { projected, limit } => {
                write!(
                    f,
                    "storage entry count {projected} exceeds policy limit {limit}"
                )
            }
            Self::QuotaExceeded {
                projected_bytes,
                limit_bytes,
            } => {
                write!(
                    f,
                    "storage bytes {projected_bytes} exceeds policy limit {limit_bytes}"
                )
            }
        }
    }
}

impl std::error::Error for StoragePolicyError {}

/// Explicit authority boundaries for browser storage operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageAuthority {
    /// Allowed storage backends. Empty means no backend authority.
    pub allowed_backends: Vec<StorageBackend>,
    /// Allowed namespace selectors.
    ///
    /// Selector forms:
    /// - exact namespace: `cache:v1`
    /// - prefix selector: `cache:*`
    /// - wildcard all: `*`
    pub allowed_namespaces: Vec<String>,
    /// Allowed operations. Empty means no operation authority.
    pub allowed_operations: Vec<StorageOperation>,
}

impl Default for StorageAuthority {
    fn default() -> Self {
        Self::deny_all()
    }
}

impl StorageAuthority {
    /// Creates an authority with no grants (default-deny posture).
    #[must_use]
    pub fn deny_all() -> Self {
        Self {
            allowed_backends: Vec::new(),
            allowed_namespaces: Vec::new(),
            allowed_operations: Vec::new(),
        }
    }

    /// Grants authority for a specific backend.
    #[must_use]
    pub fn grant_backend(mut self, backend: StorageBackend) -> Self {
        if !self.allowed_backends.contains(&backend) {
            self.allowed_backends.push(backend);
        }
        self
    }

    /// Grants authority for a namespace selector.
    #[must_use]
    pub fn grant_namespace(mut self, selector: impl Into<String>) -> Self {
        let selector = selector.into();
        if !selector.is_empty()
            && !self
                .allowed_namespaces
                .iter()
                .any(|candidate| candidate == &selector)
        {
            self.allowed_namespaces.push(selector);
        }
        self
    }

    /// Grants authority for an operation.
    #[must_use]
    pub fn grant_operation(mut self, operation: StorageOperation) -> Self {
        if !self.allowed_operations.contains(&operation) {
            self.allowed_operations.push(operation);
        }
        self
    }

    fn namespace_allowed(&self, namespace: &str) -> bool {
        self.allowed_namespaces.iter().any(|selector| {
            if selector == "*" {
                true
            } else if let Some(prefix) = selector.strip_suffix(":*") {
                namespace == prefix || namespace.starts_with(&format!("{prefix}:"))
            } else {
                selector == namespace
            }
        })
    }
}

/// Quota and shape limits for browser storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageQuotaPolicy {
    /// Maximum aggregate bytes tracked by the storage adapter.
    pub max_total_bytes: usize,
    /// Maximum key length in bytes.
    pub max_key_bytes: usize,
    /// Maximum value length in bytes.
    pub max_value_bytes: usize,
    /// Maximum namespace length in bytes.
    pub max_namespace_bytes: usize,
    /// Maximum number of entries.
    pub max_entries: usize,
}

impl Default for StorageQuotaPolicy {
    fn default() -> Self {
        Self {
            max_total_bytes: 5 * 1024 * 1024,
            max_key_bytes: 256,
            max_value_bytes: 1024 * 1024,
            max_namespace_bytes: 128,
            max_entries: 10_000,
        }
    }
}

/// Consistency contract for browser storage adapter behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageConsistencyPolicy {
    /// Reads and deletes observe writes immediately (deterministic seam).
    ImmediateReadAfterWrite,
    /// Reads observe writes immediately, but list operations may lag.
    ReadAfterWriteEventualList,
}

/// Redaction configuration for storage telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct StorageRedactionPolicy {
    /// Redact keys from telemetry.
    pub redact_keys: bool,
    /// Redact namespace labels from telemetry.
    pub redact_namespaces: bool,
    /// Redact raw value lengths from telemetry.
    pub redact_value_lengths: bool,
}

impl Default for StorageRedactionPolicy {
    fn default() -> Self {
        Self {
            redact_keys: true,
            redact_namespaces: false,
            redact_value_lengths: false,
        }
    }
}

/// Storage capability interface surfaced through [`IoCap`].
pub trait StorageIoCap: Send + Sync + Debug {
    /// Validates a request against explicit authority and baseline limits.
    fn authorize(&self, request: &StorageRequest) -> Result<(), StoragePolicyError>;

    /// Returns quota and shape limits.
    fn quota_policy(&self) -> StorageQuotaPolicy;

    /// Returns storage consistency semantics.
    fn consistency_policy(&self) -> StorageConsistencyPolicy;

    /// Returns telemetry redaction policy.
    fn redaction_policy(&self) -> StorageRedactionPolicy;
}

/// Browser-oriented storage adapter carrying explicit authority and policy.
#[derive(Debug, Clone)]
pub struct BrowserStorageIoCap {
    authority: StorageAuthority,
    quota: StorageQuotaPolicy,
    consistency: StorageConsistencyPolicy,
    redaction: StorageRedactionPolicy,
}

impl BrowserStorageIoCap {
    /// Creates a new browser storage capability adapter.
    #[must_use]
    pub fn new(
        authority: StorageAuthority,
        quota: StorageQuotaPolicy,
        consistency: StorageConsistencyPolicy,
        redaction: StorageRedactionPolicy,
    ) -> Self {
        Self {
            authority,
            quota,
            consistency,
            redaction,
        }
    }
}

/// The I/O capability trait.
///
/// Implementations of this trait provide access to I/O operations. The runtime
/// configures which implementation to use:
///
/// - Production: Real I/O via reactor (epoll/kqueue/IOCP)
/// - Lab: Virtual I/O for deterministic testing
///
/// # Example
///
/// ```ignore
/// async fn read_file(cx: &Cx, path: &str) -> io::Result<Vec<u8>> {
///     let io = cx.io().ok_or_else(|| {
///         io::Error::new(io::ErrorKind::Unsupported, "I/O not available")
///     })?;
///
///     // Open the file using the I/O capability
///     let file = io.open(path).await?;
///
///     // Read contents
///     let mut buf = Vec::new();
///     io.read_to_end(&file, &mut buf).await?;
///     Ok(buf)
/// }
/// ```
pub trait IoCap: Send + Sync + Debug {
    /// Returns true if this I/O capability supports real system I/O.
    ///
    /// Lab/test implementations return false.
    fn is_real_io(&self) -> bool;

    /// Returns the name of this I/O capability implementation.
    ///
    /// Useful for debugging and diagnostics.
    fn name(&self) -> &'static str;

    /// Returns the supported I/O features for this capability.
    fn capabilities(&self) -> IoCapabilities;

    /// Returns capability-local operation counters.
    fn stats(&self) -> IoStats {
        IoStats::default()
    }

    /// Returns the fetch adapter capability, when available.
    ///
    /// Most I/O capabilities do not expose browser fetch semantics and return
    /// `None`. Browser-oriented adapters return `Some(...)`.
    fn fetch_cap(&self) -> Option<&dyn FetchIoCap> {
        None
    }

    /// Returns the browser long-lived transport adapter capability, when available.
    ///
    /// Most I/O capabilities do not expose browser transport semantics and
    /// return `None`. Browser-oriented adapters return `Some(...)`.
    fn transport_cap(&self) -> Option<&dyn TransportIoCap> {
        None
    }

    /// Returns the storage adapter capability, when available.
    ///
    /// Most I/O capabilities do not expose browser storage semantics and return
    /// `None`. Browser-oriented adapters return `Some(...)`.
    fn storage_cap(&self) -> Option<&dyn StorageIoCap> {
        None
    }
}

/// Error returned when I/O is not available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoNotAvailable;

impl std::fmt::Display for IoNotAvailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "I/O capability not available")
    }
}

impl std::error::Error for IoNotAvailable {}

impl From<IoNotAvailable> for io::Error {
    fn from(_: IoNotAvailable) -> Self {
        Self::new(io::ErrorKind::Unsupported, "I/O capability not available")
    }
}

/// Lab I/O capability for testing.
///
/// This implementation provides virtual I/O that can be controlled by tests:
/// - Deterministic timing
/// - Fault injection
/// - Replay support
#[derive(Debug, Default)]
pub struct LabIoCap {
    submitted: AtomicU64,
    completed: AtomicU64,
}

impl LabIoCap {
    /// Creates a new lab I/O capability.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a submitted virtual I/O operation.
    pub fn record_submit(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a completed virtual I/O operation.
    pub fn record_complete(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}

impl IoCap for LabIoCap {
    fn is_real_io(&self) -> bool {
        false
    }

    fn name(&self) -> &'static str {
        "lab"
    }

    fn capabilities(&self) -> IoCapabilities {
        IoCapabilities::LAB
    }

    fn stats(&self) -> IoStats {
        IoStats {
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
        }
    }
}

impl FetchIoCap for BrowserFetchIoCap {
    fn authorize(&self, request: &FetchRequest) -> Result<(), FetchPolicyError> {
        self.authority.authorize(request)
    }

    fn timeout_policy(&self) -> FetchTimeoutPolicy {
        self.timeout
    }

    fn stream_policy(&self) -> FetchStreamPolicy {
        self.stream
    }

    fn cancellation_policy(&self) -> FetchCancellationPolicy {
        self.cancellation
    }
}

impl IoCap for BrowserFetchIoCap {
    fn is_real_io(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "browser-fetch"
    }

    fn capabilities(&self) -> IoCapabilities {
        IoCapabilities {
            file_ops: false,
            network_ops: true,
            timer_integration: true,
            deterministic: false,
        }
    }

    fn fetch_cap(&self) -> Option<&dyn FetchIoCap> {
        Some(self)
    }
}

fn is_local_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1" || host == "::1"
}

impl TransportIoCap for BrowserTransportIoCap {
    fn authorize(
        &self,
        request: &BrowserTransportRequest,
    ) -> Result<(), BrowserTransportPolicyError> {
        let (scheme, origin, host) = parse_browser_transport_url(&request.url)
            .ok_or_else(|| BrowserTransportPolicyError::InvalidUrl(request.url.clone()))?;

        if !self.support.supports(request.kind) {
            return Err(BrowserTransportPolicyError::UnsupportedKind(request.kind));
        }

        if !self.authority.allowed_kinds.contains(&request.kind) {
            return Err(BrowserTransportPolicyError::KindDenied(request.kind));
        }

        let origin_allowed = self
            .authority
            .allowed_origins
            .iter()
            .any(|candidate| candidate == "*" || candidate == &origin);
        if !origin_allowed {
            return Err(BrowserTransportPolicyError::OriginDenied(origin));
        }

        if request.subprotocols.len() > self.authority.max_subprotocol_count {
            return Err(BrowserTransportPolicyError::TooManySubprotocols {
                count: request.subprotocols.len(),
                limit: self.authority.max_subprotocol_count,
            });
        }

        if request.reconnect_attempt > self.reconnect.max_attempts {
            return Err(BrowserTransportPolicyError::ReconnectAttemptExceeded {
                attempt: request.reconnect_attempt,
                max_attempts: self.reconnect.max_attempts,
            });
        }

        match request.kind {
            BrowserTransportKind::WebSocket => {
                if scheme == "wss" {
                    return Ok(());
                }

                if scheme == "ws"
                    && self.authority.allow_insecure_localhost_ws
                    && is_local_loopback_host(&host)
                {
                    return Ok(());
                }

                Err(BrowserTransportPolicyError::InsecureScheme {
                    kind: request.kind,
                    scheme,
                })
            }
            BrowserTransportKind::WebTransport => {
                if scheme == "https" {
                    Ok(())
                } else {
                    Err(BrowserTransportPolicyError::InsecureScheme {
                        kind: request.kind,
                        scheme,
                    })
                }
            }
        }
    }

    fn support(&self) -> BrowserTransportSupport {
        self.support
    }

    fn cancellation_policy(&self) -> BrowserTransportCancellationPolicy {
        self.cancellation
    }

    fn reconnect_policy(&self) -> BrowserTransportReconnectPolicy {
        self.reconnect
    }
}

impl IoCap for BrowserTransportIoCap {
    fn is_real_io(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "browser-transport"
    }

    fn capabilities(&self) -> IoCapabilities {
        IoCapabilities {
            file_ops: false,
            network_ops: true,
            timer_integration: true,
            deterministic: false,
        }
    }

    fn transport_cap(&self) -> Option<&dyn TransportIoCap> {
        Some(self)
    }
}

impl StorageIoCap for BrowserStorageIoCap {
    fn authorize(&self, request: &StorageRequest) -> Result<(), StoragePolicyError> {
        if request.namespace.is_empty() {
            return Err(StoragePolicyError::InvalidNamespace(
                request.namespace.clone(),
            ));
        }

        if !self.authority.allowed_backends.contains(&request.backend) {
            return Err(StoragePolicyError::BackendDenied(request.backend));
        }

        if !self
            .authority
            .allowed_operations
            .contains(&request.operation)
        {
            return Err(StoragePolicyError::OperationDenied(request.operation));
        }

        if !self.authority.namespace_allowed(&request.namespace) {
            return Err(StoragePolicyError::NamespaceDenied(
                request.namespace.clone(),
            ));
        }

        let namespace_len = request.namespace.len();
        if namespace_len > self.quota.max_namespace_bytes {
            return Err(StoragePolicyError::NamespaceTooLarge {
                len: namespace_len,
                limit: self.quota.max_namespace_bytes,
            });
        }

        let key_required = matches!(
            request.operation,
            StorageOperation::Get | StorageOperation::Set | StorageOperation::Delete
        );
        if key_required && request.key.is_none() {
            return Err(StoragePolicyError::MissingKey(request.operation));
        }

        if let Some(key) = &request.key {
            if key.is_empty() {
                return Err(StoragePolicyError::MissingKey(request.operation));
            }
            if key.len() > self.quota.max_key_bytes {
                return Err(StoragePolicyError::KeyTooLarge {
                    len: key.len(),
                    limit: self.quota.max_key_bytes,
                });
            }
        }

        if request.value_len > self.quota.max_value_bytes {
            return Err(StoragePolicyError::ValueTooLarge {
                len: request.value_len,
                limit: self.quota.max_value_bytes,
            });
        }

        Ok(())
    }

    fn quota_policy(&self) -> StorageQuotaPolicy {
        self.quota
    }

    fn consistency_policy(&self) -> StorageConsistencyPolicy {
        self.consistency
    }

    fn redaction_policy(&self) -> StorageRedactionPolicy {
        self.redaction
    }
}

impl IoCap for BrowserStorageIoCap {
    fn is_real_io(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "browser-storage"
    }

    fn capabilities(&self) -> IoCapabilities {
        IoCapabilities {
            file_ops: false,
            network_ops: false,
            timer_integration: true,
            deterministic: false,
        }
    }

    fn storage_cap(&self) -> Option<&dyn StorageIoCap> {
        Some(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lab_io_cap_is_not_real() {
        let cap = LabIoCap::new();
        assert!(!cap.is_real_io());
        assert_eq!(cap.name(), "lab");
        assert_eq!(cap.capabilities(), IoCapabilities::LAB);
    }

    #[test]
    fn io_not_available_error() {
        let err = IoNotAvailable;
        let io_err: io::Error = err.into();
        assert_eq!(io_err.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    fn io_not_available_debug_clone_eq() {
        let e = IoNotAvailable;
        let dbg = format!("{e:?}");
        assert!(dbg.contains("IoNotAvailable"), "{dbg}");
        let cloned = e.clone();
        assert_eq!(e, cloned);
    }

    #[test]
    fn lab_io_cap_debug_default() {
        let c = LabIoCap::default();
        let dbg = format!("{c:?}");
        assert!(dbg.contains("LabIoCap"), "{dbg}");
    }

    #[test]
    fn lab_io_cap_stats_track_activity() {
        let cap = LabIoCap::new();
        assert_eq!(cap.stats(), IoStats::default());
        cap.record_submit();
        cap.record_submit();
        cap.record_complete();
        assert_eq!(
            cap.stats(),
            IoStats {
                submitted: 2,
                completed: 1
            }
        );
    }

    #[test]
    fn fetch_authority_allows_expected_origin_and_method() {
        let authority = FetchAuthority::deny_all()
            .grant_origin("https://api.example.com")
            .grant_method(FetchMethod::Get)
            .grant_method(FetchMethod::Post)
            .with_max_header_count(8);
        let request = FetchRequest::new(FetchMethod::Get, "https://api.example.com/v1/data")
            .with_header("x-trace-id", "t-1");
        assert_eq!(authority.authorize(&request), Ok(()));
    }

    #[test]
    fn fetch_authority_default_is_deny_all() {
        let authority = FetchAuthority::default();
        let request = FetchRequest::new(FetchMethod::Get, "https://api.example.com/v1/data");
        assert_eq!(
            authority.authorize(&request),
            Err(FetchPolicyError::OriginDenied(
                "https://api.example.com".to_owned()
            ))
        );
    }

    #[test]
    fn fetch_authority_denies_unlisted_origin() {
        let authority = FetchAuthority {
            allowed_origins: vec!["https://api.example.com".to_owned()],
            ..FetchAuthority::default()
        };
        let request = FetchRequest::new(FetchMethod::Get, "https://evil.example.com/v1/data");
        assert_eq!(
            authority.authorize(&request),
            Err(FetchPolicyError::OriginDenied(
                "https://evil.example.com".to_owned()
            ))
        );
    }

    #[test]
    fn fetch_authority_denies_credentials_when_disallowed() {
        let authority = FetchAuthority::deny_all()
            .grant_origin("https://api.example.com")
            .grant_method(FetchMethod::Get)
            .with_max_header_count(4);
        let request = FetchRequest::new(FetchMethod::Get, "https://api.example.com/v1/data")
            .with_credentials();
        assert_eq!(
            authority.authorize(&request),
            Err(FetchPolicyError::CredentialsDenied)
        );
    }

    #[test]
    fn fetch_authority_allows_credentials_with_explicit_grant() {
        let authority = FetchAuthority::deny_all()
            .grant_origin("https://api.example.com")
            .grant_method(FetchMethod::Get)
            .with_max_header_count(4)
            .with_credentials_allowed();
        let request = FetchRequest::new(FetchMethod::Get, "https://api.example.com/v1/data")
            .with_credentials();
        assert_eq!(authority.authorize(&request), Ok(()));
    }

    #[test]
    fn fetch_authority_rejects_invalid_url() {
        let authority = FetchAuthority::default();
        let request = FetchRequest::new(FetchMethod::Get, "not-a-url");
        assert_eq!(
            authority.authorize(&request),
            Err(FetchPolicyError::InvalidUrl("not-a-url".to_owned()))
        );
    }

    #[test]
    fn browser_fetch_cap_exposes_policies_through_iocap() {
        let timeout = FetchTimeoutPolicy {
            request_timeout_ms: 15_000,
            first_byte_timeout_ms: 2_000,
            between_chunks_timeout_ms: 1_500,
        };
        let stream = FetchStreamPolicy {
            max_request_body_bytes: 1024,
            max_response_body_bytes: 2048,
            max_response_header_bytes: 512,
        };
        let cap = BrowserFetchIoCap::new(
            FetchAuthority::default(),
            timeout,
            stream,
            FetchCancellationPolicy::AbortSignalWithDrain,
        );

        let io_cap: &dyn IoCap = &cap;
        let fetch_cap = io_cap.fetch_cap().expect("fetch cap should be present");
        assert_eq!(fetch_cap.timeout_policy(), timeout);
        assert_eq!(fetch_cap.stream_policy(), stream);
        assert_eq!(
            fetch_cap.cancellation_policy(),
            FetchCancellationPolicy::AbortSignalWithDrain
        );
    }

    fn strict_transport_cap(
        support: BrowserTransportSupport,
        localhost_insecure_ws: bool,
    ) -> BrowserTransportIoCap {
        let mut authority = BrowserTransportAuthority::deny_all()
            .grant_origin("wss://chat.example.com")
            .grant_origin("https://transport.example.com")
            .grant_kind(BrowserTransportKind::WebSocket)
            .grant_kind(BrowserTransportKind::WebTransport)
            .with_max_subprotocol_count(2);
        if localhost_insecure_ws {
            authority = authority.with_localhost_insecure_ws();
        }

        BrowserTransportIoCap::new(
            authority,
            support,
            BrowserTransportCancellationPolicy::CloseThenAbort,
            BrowserTransportReconnectPolicy {
                max_attempts: 2,
                base_delay_ms: 100,
                max_delay_ms: 1_000,
                jitter_ms: 0,
            },
        )
    }

    #[test]
    fn transport_authority_default_is_deny_all() {
        let cap = BrowserTransportIoCap::new(
            BrowserTransportAuthority::default(),
            BrowserTransportSupport::FULL,
            BrowserTransportCancellationPolicy::CloseThenAbort,
            BrowserTransportReconnectPolicy::default(),
        );
        let request =
            BrowserTransportRequest::new(BrowserTransportKind::WebSocket, "wss://chat.example.com");

        assert_eq!(
            cap.authorize(&request),
            Err(BrowserTransportPolicyError::KindDenied(
                BrowserTransportKind::WebSocket
            ))
        );
    }

    #[test]
    fn transport_policy_rejects_insecure_remote_ws() {
        let cap = BrowserTransportIoCap::new(
            BrowserTransportAuthority::deny_all()
                .grant_origin("ws://chat.example.com")
                .grant_kind(BrowserTransportKind::WebSocket)
                .with_max_subprotocol_count(2),
            BrowserTransportSupport::WEBSOCKET_ONLY,
            BrowserTransportCancellationPolicy::CloseThenAbort,
            BrowserTransportReconnectPolicy::default(),
        );
        let request =
            BrowserTransportRequest::new(BrowserTransportKind::WebSocket, "ws://chat.example.com");

        assert_eq!(
            cap.authorize(&request),
            Err(BrowserTransportPolicyError::InsecureScheme {
                kind: BrowserTransportKind::WebSocket,
                scheme: "ws".to_owned()
            })
        );
    }

    #[test]
    fn transport_policy_allows_localhost_ws_when_explicitly_granted() {
        let cap = BrowserTransportIoCap::new(
            BrowserTransportAuthority::deny_all()
                .grant_origin("ws://localhost:8080")
                .grant_kind(BrowserTransportKind::WebSocket)
                .with_max_subprotocol_count(2)
                .with_localhost_insecure_ws(),
            BrowserTransportSupport::WEBSOCKET_ONLY,
            BrowserTransportCancellationPolicy::CloseThenAbort,
            BrowserTransportReconnectPolicy::default(),
        );
        let request =
            BrowserTransportRequest::new(BrowserTransportKind::WebSocket, "ws://localhost:8080");
        assert_eq!(cap.authorize(&request), Ok(()));
    }

    #[test]
    fn transport_policy_enforces_support_matrix() {
        let cap = strict_transport_cap(BrowserTransportSupport::WEBSOCKET_ONLY, false);
        let request = BrowserTransportRequest::new(
            BrowserTransportKind::WebTransport,
            "https://transport.example.com/session",
        );

        assert_eq!(
            cap.authorize(&request),
            Err(BrowserTransportPolicyError::UnsupportedKind(
                BrowserTransportKind::WebTransport
            ))
        );
    }

    #[test]
    fn transport_policy_enforces_reconnect_limit() {
        let cap = strict_transport_cap(BrowserTransportSupport::FULL, false);
        let request = BrowserTransportRequest::new(
            BrowserTransportKind::WebTransport,
            "https://transport.example.com/session",
        )
        .with_reconnect_attempt(3);

        assert_eq!(
            cap.authorize(&request),
            Err(BrowserTransportPolicyError::ReconnectAttemptExceeded {
                attempt: 3,
                max_attempts: 2
            })
        );
    }

    #[test]
    fn browser_transport_cap_exposes_policies_through_iocap() {
        let reconnect = BrowserTransportReconnectPolicy {
            max_attempts: 4,
            base_delay_ms: 250,
            max_delay_ms: 4_000,
            jitter_ms: 0,
        };
        let cap = BrowserTransportIoCap::new(
            BrowserTransportAuthority::deny_all()
                .grant_origin("wss://chat.example.com")
                .grant_kind(BrowserTransportKind::WebSocket)
                .with_max_subprotocol_count(3),
            BrowserTransportSupport::WEBSOCKET_ONLY,
            BrowserTransportCancellationPolicy::CloseThenAbort,
            reconnect,
        );

        let io_cap: &dyn IoCap = &cap;
        let transport_cap = io_cap
            .transport_cap()
            .expect("browser transport cap should be present");
        assert_eq!(
            transport_cap.support(),
            BrowserTransportSupport::WEBSOCKET_ONLY
        );
        assert_eq!(
            transport_cap.cancellation_policy(),
            BrowserTransportCancellationPolicy::CloseThenAbort
        );
        assert_eq!(transport_cap.reconnect_policy(), reconnect);
    }

    #[test]
    fn storage_authority_default_is_deny_all() {
        let cap = BrowserStorageIoCap::new(
            StorageAuthority::default(),
            StorageQuotaPolicy::default(),
            StorageConsistencyPolicy::ImmediateReadAfterWrite,
            StorageRedactionPolicy::default(),
        );
        let request = StorageRequest::get(StorageBackend::IndexedDb, "cache:v1", "entry");
        assert_eq!(
            cap.authorize(&request),
            Err(StoragePolicyError::BackendDenied(StorageBackend::IndexedDb))
        );
    }

    #[test]
    fn storage_authority_supports_namespace_prefix_rules() {
        let cap = BrowserStorageIoCap::new(
            StorageAuthority::deny_all()
                .grant_backend(StorageBackend::IndexedDb)
                .grant_operation(StorageOperation::Get)
                .grant_namespace("cache:*"),
            StorageQuotaPolicy::default(),
            StorageConsistencyPolicy::ImmediateReadAfterWrite,
            StorageRedactionPolicy::default(),
        );

        let allowed = StorageRequest::get(StorageBackend::IndexedDb, "cache:user:42", "profile");
        assert_eq!(cap.authorize(&allowed), Ok(()));

        let denied = StorageRequest::get(StorageBackend::IndexedDb, "session:v1", "profile");
        assert_eq!(
            cap.authorize(&denied),
            Err(StoragePolicyError::NamespaceDenied("session:v1".to_owned()))
        );
    }

    #[test]
    fn storage_authorize_enforces_key_and_value_limits() {
        let cap = BrowserStorageIoCap::new(
            StorageAuthority::deny_all()
                .grant_backend(StorageBackend::LocalStorage)
                .grant_operation(StorageOperation::Set)
                .grant_namespace("*"),
            StorageQuotaPolicy {
                max_key_bytes: 4,
                max_value_bytes: 3,
                ..StorageQuotaPolicy::default()
            },
            StorageConsistencyPolicy::ImmediateReadAfterWrite,
            StorageRedactionPolicy::default(),
        );

        let missing_key = StorageRequest::new(
            StorageBackend::LocalStorage,
            StorageOperation::Set,
            "prefs:v1",
        )
        .with_value_len(2);
        assert_eq!(
            cap.authorize(&missing_key),
            Err(StoragePolicyError::MissingKey(StorageOperation::Set))
        );

        let long_key = StorageRequest::set(StorageBackend::LocalStorage, "prefs:v1", "abcde", 2);
        assert_eq!(
            cap.authorize(&long_key),
            Err(StoragePolicyError::KeyTooLarge { len: 5, limit: 4 })
        );

        let long_value = StorageRequest::set(StorageBackend::LocalStorage, "prefs:v1", "k1", 5);
        assert_eq!(
            cap.authorize(&long_value),
            Err(StoragePolicyError::ValueTooLarge { len: 5, limit: 3 })
        );
    }

    #[test]
    fn browser_storage_cap_exposes_policies_through_iocap() {
        let quota = StorageQuotaPolicy {
            max_total_bytes: 4096,
            max_key_bytes: 64,
            max_value_bytes: 2048,
            max_namespace_bytes: 32,
            max_entries: 64,
        };
        let redaction = StorageRedactionPolicy {
            redact_keys: true,
            redact_namespaces: true,
            redact_value_lengths: false,
        };
        let cap = BrowserStorageIoCap::new(
            StorageAuthority::deny_all()
                .grant_backend(StorageBackend::IndexedDb)
                .grant_operation(StorageOperation::Get)
                .grant_namespace("cache:*"),
            quota,
            StorageConsistencyPolicy::ImmediateReadAfterWrite,
            redaction,
        );
        let io_cap: &dyn IoCap = &cap;
        let storage_cap = io_cap.storage_cap().expect("storage cap should be present");
        assert_eq!(storage_cap.quota_policy(), quota);
        assert_eq!(
            storage_cap.consistency_policy(),
            StorageConsistencyPolicy::ImmediateReadAfterWrite
        );
        assert_eq!(storage_cap.redaction_policy(), redaction);
    }
}
