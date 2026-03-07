//! Browser storage adapter with explicit authority and deterministic test seam.
//!
//! This module provides a policy-enforced in-memory bridge for browser storage
//! semantics (IndexedDB/localStorage style APIs). It is intentionally
//! deterministic: storage is backed by `BTreeMap` and all key enumeration order
//! is stable.

use crate::io::cap::{
    BrowserStorageIoCap, StorageBackend, StorageConsistencyPolicy, StorageIoCap, StorageOperation,
    StoragePolicyError, StorageRequest,
};
#[cfg(target_arch = "wasm32")]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(target_arch = "wasm32")]
use web_sys::Storage;

/// Error returned by browser storage operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowserStorageError {
    /// Policy validation failed.
    Policy(StoragePolicyError),
    /// Backend is temporarily unavailable in current execution context.
    BackendUnavailable(StorageBackend),
    /// Host-backed backend returned an operation error.
    HostBackend {
        /// Backend that produced the error.
        backend: StorageBackend,
        /// Storage operation that failed.
        operation: StorageOperation,
        /// Backend-provided diagnostic message.
        message: String,
    },
}

impl std::fmt::Display for BrowserStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Policy(error) => write!(f, "{error}"),
            Self::BackendUnavailable(backend) => {
                write!(f, "storage backend unavailable: {backend:?}")
            }
            Self::HostBackend {
                backend,
                operation,
                message,
            } => write!(
                f,
                "storage host backend error ({backend:?}, {operation:?}): {message}"
            ),
        }
    }
}

impl std::error::Error for BrowserStorageError {}

impl From<StoragePolicyError> for BrowserStorageError {
    fn from(error: StoragePolicyError) -> Self {
        Self::Policy(error)
    }
}

/// Structured storage telemetry event with redaction-aware fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageEvent {
    /// Operation that was attempted.
    pub operation: StorageOperation,
    /// Backend targeted by the operation.
    pub backend: StorageBackend,
    /// Namespace label (possibly redacted).
    pub namespace_label: String,
    /// Key label (possibly redacted).
    pub key_label: Option<String>,
    /// Value length metadata (possibly redacted).
    pub value_len: Option<usize>,
    /// Event outcome.
    pub outcome: StorageEventOutcome,
    /// Deterministic reason code for policy and availability diagnostics.
    pub reason_code: StorageEventReasonCode,
}

/// Deterministic outcome classification for storage telemetry events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageEventOutcome {
    /// Request passed policy checks and was applied.
    Allowed,
    /// Request was denied by policy.
    Denied,
}

/// Stable reason code attached to storage telemetry events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageEventReasonCode {
    /// Request passed policy checks and was applied.
    Allowed,
    /// Namespace was empty/invalid.
    InvalidNamespace,
    /// Backend was outside allowed policy.
    BackendDenied,
    /// Namespace was outside allowed policy.
    NamespaceDenied,
    /// Operation was outside allowed policy.
    OperationDenied,
    /// Required key was missing.
    MissingKey,
    /// Key exceeded configured length limits.
    KeyTooLarge,
    /// Value exceeded configured length limits.
    ValueTooLarge,
    /// Namespace exceeded configured length limits.
    NamespaceTooLarge,
    /// Entry count would exceed configured limits.
    EntryCountExceeded,
    /// Aggregate bytes would exceed configured limits.
    QuotaExceeded,
    /// Backend is unavailable in this execution context.
    BackendUnavailable,
    /// Host backend returned an operation error.
    HostBackendError,
}

/// Host-backed browser storage implementation contract.
///
/// This allows the storage adapter to route specific backends (for example
/// `localStorage` in wasm browsers) to concrete host facilities while keeping
/// policy checks, telemetry, and deterministic behavior in one place.
pub trait StorageHostBackend: std::fmt::Debug + Send + Sync {
    /// Writes a value for the given namespace/key.
    fn set(&self, namespace: &str, key: &str, value: &[u8]) -> Result<(), String>;
    /// Reads a value for the given namespace/key.
    fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, String>;
    /// Deletes a key and returns whether a value existed.
    fn delete(&self, namespace: &str, key: &str) -> Result<bool, String>;
    /// Lists keys in a namespace.
    fn list_keys(&self, namespace: &str) -> Result<Vec<String>, String>;
    /// Clears a namespace and returns removed entry count.
    fn clear_namespace(&self, namespace: &str) -> Result<usize, String>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StorageKey {
    backend: StorageBackend,
    namespace: String,
    key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StorageNamespaceKey {
    backend: StorageBackend,
    namespace: String,
}

/// Deterministic browser storage adapter used for policy enforcement and tests.
#[derive(Debug, Clone)]
pub struct BrowserStorageAdapter {
    cap: BrowserStorageIoCap,
    entries: BTreeMap<StorageKey, Vec<u8>>,
    list_snapshot: BTreeMap<StorageNamespaceKey, Vec<String>>,
    host_backends: BTreeMap<StorageBackend, Arc<dyn StorageHostBackend>>,
    unavailable_backends: BTreeMap<StorageBackend, bool>,
    used_bytes: usize,
    events: Vec<StorageEvent>,
}

impl BrowserStorageAdapter {
    /// Creates a new deterministic storage adapter.
    #[must_use]
    pub fn new(cap: BrowserStorageIoCap) -> Self {
        Self {
            cap,
            entries: BTreeMap::new(),
            list_snapshot: BTreeMap::new(),
            host_backends: BTreeMap::new(),
            unavailable_backends: BTreeMap::new(),
            used_bytes: 0,
            events: Vec::new(),
        }
    }

    /// Returns the configured capability adapter.
    #[must_use]
    pub fn cap(&self) -> &BrowserStorageIoCap {
        &self.cap
    }

    /// Returns currently tracked aggregate storage bytes.
    #[must_use]
    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    /// Returns the current deterministic entry count.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Returns collected storage telemetry events.
    #[must_use]
    pub fn events(&self) -> &[StorageEvent] {
        &self.events
    }

    /// Registers a host-backed implementation for a specific storage backend.
    ///
    /// When present, storage operations for `backend` are routed through this
    /// implementation after policy authorization.
    pub fn register_host_backend(
        &mut self,
        backend: StorageBackend,
        host_backend: Arc<dyn StorageHostBackend>,
    ) {
        self.host_backends.insert(backend, host_backend);
    }

    /// Registers the default wasm `localStorage` host backend.
    #[cfg(target_arch = "wasm32")]
    pub fn register_wasm_local_storage_backend(&mut self) {
        self.register_host_backend(
            StorageBackend::LocalStorage,
            Arc::new(LocalStorageHostBackend),
        );
    }

    /// Removes any registered host-backed implementation for `backend`.
    pub fn unregister_host_backend(
        &mut self,
        backend: StorageBackend,
    ) -> Option<Arc<dyn StorageHostBackend>> {
        self.host_backends.remove(&backend)
    }

    /// Returns whether a host-backed implementation is registered for `backend`.
    #[must_use]
    pub fn has_host_backend(&self, backend: StorageBackend) -> bool {
        self.host_backends.contains_key(&backend)
    }

    /// Configures deterministic availability for a backend.
    ///
    /// When set to unavailable, all operations targeting the backend fail with
    /// [`BrowserStorageError::BackendUnavailable`] after authority validation.
    pub fn set_backend_available(&mut self, backend: StorageBackend, available: bool) {
        if available {
            self.unavailable_backends.remove(&backend);
        } else {
            self.unavailable_backends.insert(backend, false);
        }
    }

    /// Returns whether a backend is currently marked available.
    #[must_use]
    pub fn backend_available(&self, backend: StorageBackend) -> bool {
        self.unavailable_backends
            .get(&backend)
            .copied()
            .unwrap_or(true)
    }

    /// Deterministically forces list-view convergence for a namespace.
    pub fn flush_namespace_list_view(
        &mut self,
        backend: StorageBackend,
        namespace: impl Into<String>,
    ) {
        let namespace = namespace.into();
        self.recompute_list_snapshot(backend, &namespace);
    }

    /// Stores a value under `(backend, namespace, key)`.
    pub fn set(
        &mut self,
        backend: StorageBackend,
        namespace: impl Into<String>,
        key: impl Into<String>,
        value: Vec<u8>,
    ) -> Result<(), BrowserStorageError> {
        let namespace = namespace.into();
        let key = key.into();
        let request = StorageRequest::set(backend, namespace.clone(), key.clone(), value.len());
        self.authorize_and_record(&request)?;

        let quota = self.cap.quota_policy();
        let storage_key = StorageKey {
            backend,
            namespace: namespace.clone(),
            key: key.clone(),
        };
        let new_entry_size = entry_size(&namespace, &key, value.len());
        let old_entry_size = self
            .entries
            .get(&storage_key)
            .map_or(0, |old| entry_size(&namespace, &key, old.len()));

        let projected_entries = if self.entries.contains_key(&storage_key) {
            self.entries.len()
        } else {
            self.entries.len() + 1
        };
        if projected_entries > quota.max_entries {
            return self.policy_error(
                &request,
                StoragePolicyError::EntryCountExceeded {
                    projected: projected_entries,
                    limit: quota.max_entries,
                },
            );
        }

        let projected_bytes = self.used_bytes - old_entry_size + new_entry_size;
        if projected_bytes > quota.max_total_bytes {
            return self.policy_error(
                &request,
                StoragePolicyError::QuotaExceeded {
                    projected_bytes,
                    limit_bytes: quota.max_total_bytes,
                },
            );
        }

        if let Some(host_backend) = self.host_backend(backend) {
            if let Err(message) = host_backend.set(&namespace, &key, &value) {
                return self.host_backend_error(&request, message);
            }
        }

        self.used_bytes = projected_bytes;
        self.entries.insert(storage_key, value);
        Ok(())
    }

    /// Reads a value by `(backend, namespace, key)`.
    pub fn get(
        &mut self,
        backend: StorageBackend,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Option<Vec<u8>>, BrowserStorageError> {
        let namespace = namespace.into();
        let key = key.into();
        let request = StorageRequest::get(backend, namespace.clone(), key.clone());
        self.authorize_and_record(&request)?;

        let storage_key = StorageKey {
            backend,
            namespace,
            key,
        };
        if let Some(host_backend) = self.host_backend(backend) {
            let value = match host_backend.get(&storage_key.namespace, &storage_key.key) {
                Ok(value) => value,
                Err(message) => return self.host_backend_error(&request, message),
            };
            self.sync_entry_cache(&storage_key, value.as_ref());
            return Ok(value);
        }

        Ok(self.entries.get(&storage_key).cloned())
    }

    /// Deletes a single key.
    pub fn delete(
        &mut self,
        backend: StorageBackend,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<bool, BrowserStorageError> {
        let namespace = namespace.into();
        let key = key.into();
        let request = StorageRequest::delete(backend, namespace.clone(), key.clone());
        self.authorize_and_record(&request)?;

        let storage_key = StorageKey {
            backend,
            namespace: namespace.clone(),
            key: key.clone(),
        };

        if let Some(host_backend) = self.host_backend(backend) {
            let deleted = match host_backend.delete(&namespace, &key) {
                Ok(deleted) => deleted,
                Err(message) => return self.host_backend_error(&request, message),
            };
            self.remove_cached_entry(&storage_key);
            return Ok(deleted);
        }

        let removed = self.entries.remove(&storage_key);
        if let Some(old) = removed {
            self.used_bytes =
                self.used_bytes
                    .saturating_sub(entry_size(&namespace, &key, old.len()));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Lists keys in deterministic sorted order for a namespace.
    pub fn list_keys(
        &mut self,
        backend: StorageBackend,
        namespace: impl Into<String>,
    ) -> Result<Vec<String>, BrowserStorageError> {
        let namespace = namespace.into();
        let request = StorageRequest::list_keys(backend, namespace.clone());
        self.authorize_and_record(&request)?;

        if let Some(host_backend) = self.host_backend(backend) {
            if self.cap.consistency_policy() == StorageConsistencyPolicy::ImmediateReadAfterWrite {
                return self.host_backend_list_keys(&request, &*host_backend, &namespace);
            }

            let namespace_key = StorageNamespaceKey {
                backend,
                namespace: namespace.clone(),
            };
            let visible = self
                .list_snapshot
                .get(&namespace_key)
                .cloned()
                .unwrap_or_default();

            let next = self.host_backend_list_keys(&request, &*host_backend, &namespace)?;
            self.list_snapshot.insert(namespace_key, next);
            return Ok(visible);
        }

        if self.cap.consistency_policy() == StorageConsistencyPolicy::ImmediateReadAfterWrite {
            return Ok(self.collect_namespace_keys(backend, &namespace));
        }

        let namespace_key = StorageNamespaceKey {
            backend,
            namespace: namespace.clone(),
        };
        let visible = self
            .list_snapshot
            .get(&namespace_key)
            .cloned()
            .unwrap_or_default();

        // Deterministic eventual-consistency seam: this call may return a
        // stale list, but advances the snapshot so the next list converges.
        self.recompute_list_snapshot(backend, &namespace);
        Ok(visible)
    }

    /// Clears all keys in a namespace and returns number of removed entries.
    pub fn clear_namespace(
        &mut self,
        backend: StorageBackend,
        namespace: impl Into<String>,
    ) -> Result<usize, BrowserStorageError> {
        let namespace = namespace.into();
        let request = StorageRequest::clear_namespace(backend, namespace.clone());
        self.authorize_and_record(&request)?;

        if let Some(host_backend) = self.host_backend(backend) {
            let removed_count = match host_backend.clear_namespace(&namespace) {
                Ok(removed_count) => removed_count,
                Err(message) => return self.host_backend_error(&request, message),
            };
            self.remove_cached_namespace(backend, &namespace);
            return Ok(removed_count);
        }

        let keys_to_remove: Vec<StorageKey> = self
            .entries
            .keys()
            .filter(|candidate| candidate.backend == backend && candidate.namespace == namespace)
            .cloned()
            .collect();
        let removed_count = keys_to_remove.len();

        for key in keys_to_remove {
            if let Some(value) = self.entries.remove(&key) {
                self.used_bytes = self.used_bytes.saturating_sub(entry_size(
                    &key.namespace,
                    &key.key,
                    value.len(),
                ));
            }
        }

        Ok(removed_count)
    }

    fn authorize_and_record(
        &mut self,
        request: &StorageRequest,
    ) -> Result<(), BrowserStorageError> {
        match self.cap.authorize(request) {
            Ok(()) => {
                if !self.backend_available(request.backend) {
                    return self.backend_unavailable(request);
                }
                self.record_event(
                    request,
                    StorageEventOutcome::Allowed,
                    StorageEventReasonCode::Allowed,
                );
                Ok(())
            }
            Err(error) => self.policy_error(request, error),
        }
    }

    fn policy_error<T>(
        &mut self,
        request: &StorageRequest,
        error: StoragePolicyError,
    ) -> Result<T, BrowserStorageError> {
        self.record_event(
            request,
            StorageEventOutcome::Denied,
            reason_code_for_policy_error(&error),
        );
        Err(BrowserStorageError::Policy(error))
    }

    fn backend_unavailable<T>(
        &mut self,
        request: &StorageRequest,
    ) -> Result<T, BrowserStorageError> {
        self.record_event(
            request,
            StorageEventOutcome::Denied,
            StorageEventReasonCode::BackendUnavailable,
        );
        Err(BrowserStorageError::BackendUnavailable(request.backend))
    }

    fn host_backend_error<T>(
        &mut self,
        request: &StorageRequest,
        message: String,
    ) -> Result<T, BrowserStorageError> {
        self.record_event(
            request,
            StorageEventOutcome::Denied,
            StorageEventReasonCode::HostBackendError,
        );
        Err(BrowserStorageError::HostBackend {
            backend: request.backend,
            operation: request.operation,
            message,
        })
    }

    fn host_backend(&self, backend: StorageBackend) -> Option<Arc<dyn StorageHostBackend>> {
        self.host_backends.get(&backend).cloned()
    }

    fn host_backend_list_keys(
        &mut self,
        request: &StorageRequest,
        backend: &dyn StorageHostBackend,
        namespace: &str,
    ) -> Result<Vec<String>, BrowserStorageError> {
        let mut keys = match backend.list_keys(namespace) {
            Ok(keys) => keys,
            Err(message) => return self.host_backend_error(request, message),
        };
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    fn sync_entry_cache(&mut self, storage_key: &StorageKey, value: Option<&Vec<u8>>) {
        self.remove_cached_entry(storage_key);
        if let Some(value) = value {
            self.used_bytes = self.used_bytes.saturating_add(entry_size(
                &storage_key.namespace,
                &storage_key.key,
                value.len(),
            ));
            self.entries.insert(storage_key.clone(), value.clone());
        }
    }

    fn remove_cached_entry(&mut self, storage_key: &StorageKey) {
        if let Some(previous) = self.entries.remove(storage_key) {
            self.used_bytes = self.used_bytes.saturating_sub(entry_size(
                &storage_key.namespace,
                &storage_key.key,
                previous.len(),
            ));
        }
    }

    fn remove_cached_namespace(&mut self, backend: StorageBackend, namespace: &str) {
        let keys_to_remove: Vec<StorageKey> = self
            .entries
            .keys()
            .filter(|candidate| candidate.backend == backend && candidate.namespace == namespace)
            .cloned()
            .collect();
        for key in keys_to_remove {
            self.remove_cached_entry(&key);
        }
    }

    fn collect_namespace_keys(&self, backend: StorageBackend, namespace: &str) -> Vec<String> {
        self.entries
            .keys()
            .filter(|candidate| candidate.backend == backend && candidate.namespace == namespace)
            .map(|candidate| candidate.key.clone())
            .collect()
    }

    fn recompute_list_snapshot(&mut self, backend: StorageBackend, namespace: &str) {
        let key = StorageNamespaceKey {
            backend,
            namespace: namespace.to_owned(),
        };
        self.list_snapshot
            .insert(key, self.collect_namespace_keys(backend, namespace));
    }

    fn record_event(
        &mut self,
        request: &StorageRequest,
        outcome: StorageEventOutcome,
        reason_code: StorageEventReasonCode,
    ) {
        let redaction = self.cap.redaction_policy();
        let namespace_label = if redaction.redact_namespaces {
            format!("namespace[len:{}]", request.namespace.len())
        } else {
            request.namespace.clone()
        };
        let key_label = request.key.as_ref().map(|key| {
            if redaction.redact_keys {
                format!("key[len:{}]", key.len())
            } else {
                key.clone()
            }
        });
        let value_len = if redaction.redact_value_lengths {
            None
        } else {
            Some(request.value_len)
        };

        self.events.push(StorageEvent {
            operation: request.operation,
            backend: request.backend,
            namespace_label,
            key_label,
            value_len,
            outcome,
            reason_code,
        });
    }
}

fn reason_code_for_policy_error(error: &StoragePolicyError) -> StorageEventReasonCode {
    match error {
        StoragePolicyError::InvalidNamespace(_) => StorageEventReasonCode::InvalidNamespace,
        StoragePolicyError::BackendDenied(_) => StorageEventReasonCode::BackendDenied,
        StoragePolicyError::NamespaceDenied(_) => StorageEventReasonCode::NamespaceDenied,
        StoragePolicyError::OperationDenied(_) => StorageEventReasonCode::OperationDenied,
        StoragePolicyError::MissingKey(_) => StorageEventReasonCode::MissingKey,
        StoragePolicyError::KeyTooLarge { .. } => StorageEventReasonCode::KeyTooLarge,
        StoragePolicyError::ValueTooLarge { .. } => StorageEventReasonCode::ValueTooLarge,
        StoragePolicyError::NamespaceTooLarge { .. } => StorageEventReasonCode::NamespaceTooLarge,
        StoragePolicyError::EntryCountExceeded { .. } => StorageEventReasonCode::EntryCountExceeded,
        StoragePolicyError::QuotaExceeded { .. } => StorageEventReasonCode::QuotaExceeded,
    }
}

fn entry_size(namespace: &str, key: &str, value_len: usize) -> usize {
    namespace.len() + key.len() + value_len
}

/// WASM host backend that persists values in browser `localStorage`.
#[cfg(target_arch = "wasm32")]
#[derive(Debug, Default)]
pub struct LocalStorageHostBackend;

#[cfg(target_arch = "wasm32")]
impl LocalStorageHostBackend {
    const KEY_PREFIX: &'static str = "asupersync:storage:v1:";

    fn with_storage<T>(f: impl FnOnce(Storage) -> Result<T, String>) -> Result<T, String> {
        let window = web_sys::window().ok_or_else(|| "window is unavailable".to_owned())?;
        let storage = window
            .local_storage()
            .map_err(|error| format!("failed to access localStorage: {error:?}"))?
            .ok_or_else(|| "localStorage is unavailable".to_owned())?;
        f(storage)
    }

    fn key_prefix(namespace: &str) -> String {
        let encoded_namespace = URL_SAFE_NO_PAD.encode(namespace.as_bytes());
        format!("{}{encoded_namespace}:", Self::KEY_PREFIX)
    }

    fn encode_storage_key(namespace: &str, key: &str) -> String {
        let mut prefixed = Self::key_prefix(namespace);
        prefixed.push_str(&URL_SAFE_NO_PAD.encode(key.as_bytes()));
        prefixed
    }

    fn decode_key_segment(encoded: &str) -> Option<String> {
        URL_SAFE_NO_PAD
            .decode(encoded)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
    }

    fn decode_storage_key(full_key: &str, namespace: &str) -> Option<String> {
        let prefix = Self::key_prefix(namespace);
        full_key
            .strip_prefix(&prefix)
            .and_then(Self::decode_key_segment)
    }
}

#[cfg(target_arch = "wasm32")]
impl StorageHostBackend for LocalStorageHostBackend {
    fn set(&self, namespace: &str, key: &str, value: &[u8]) -> Result<(), String> {
        let storage_key = Self::encode_storage_key(namespace, key);
        let encoded_value = URL_SAFE_NO_PAD.encode(value);
        Self::with_storage(|storage| {
            storage
                .set_item(&storage_key, &encoded_value)
                .map_err(|error| format!("localStorage set_item failed: {error:?}"))
        })
    }

    fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
        let storage_key = Self::encode_storage_key(namespace, key);
        Self::with_storage(|storage| {
            let encoded = storage
                .get_item(&storage_key)
                .map_err(|error| format!("localStorage get_item failed: {error:?}"))?;
            encoded
                .map(|payload| {
                    URL_SAFE_NO_PAD
                        .decode(payload.as_bytes())
                        .map_err(|error| format!("failed to decode localStorage payload: {error}"))
                })
                .transpose()
        })
    }

    fn delete(&self, namespace: &str, key: &str) -> Result<bool, String> {
        let storage_key = Self::encode_storage_key(namespace, key);
        Self::with_storage(|storage| {
            let existed = storage
                .get_item(&storage_key)
                .map_err(|error| format!("localStorage get_item failed: {error:?}"))?
                .is_some();
            storage
                .remove_item(&storage_key)
                .map_err(|error| format!("localStorage remove_item failed: {error:?}"))?;
            Ok(existed)
        })
    }

    fn list_keys(&self, namespace: &str) -> Result<Vec<String>, String> {
        Self::with_storage(|storage| {
            let mut keys = Vec::new();
            let len = storage
                .length()
                .map_err(|error| format!("localStorage length failed: {error:?}"))?;
            for index in 0..len {
                let maybe_key = storage
                    .key(index)
                    .map_err(|error| format!("localStorage key({index}) failed: {error:?}"))?;
                if let Some(full_key) = maybe_key {
                    if let Some(decoded) = Self::decode_storage_key(&full_key, namespace) {
                        keys.push(decoded);
                    }
                }
            }
            Ok(keys)
        })
    }

    fn clear_namespace(&self, namespace: &str) -> Result<usize, String> {
        let keys = self.list_keys(namespace)?;
        for key in &keys {
            let storage_key = Self::encode_storage_key(namespace, key);
            Self::with_storage(|storage| {
                storage
                    .remove_item(&storage_key)
                    .map_err(|error| format!("localStorage remove_item failed: {error:?}"))
            })?;
        }
        Ok(keys.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::cap::{
        StorageAuthority, StorageConsistencyPolicy, StorageOperation, StorageQuotaPolicy,
        StorageRedactionPolicy,
    };
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct MockHostBackend {
        entries: Mutex<BTreeMap<(String, String), Vec<u8>>>,
    }

    impl StorageHostBackend for MockHostBackend {
        fn set(&self, namespace: &str, key: &str, value: &[u8]) -> Result<(), String> {
            self.entries
                .lock()
                .expect("host backend lock poisoned")
                .insert((namespace.to_owned(), key.to_owned()), value.to_vec());
            Ok(())
        }

        fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
            Ok(self
                .entries
                .lock()
                .expect("host backend lock poisoned")
                .get(&(namespace.to_owned(), key.to_owned()))
                .cloned())
        }

        fn delete(&self, namespace: &str, key: &str) -> Result<bool, String> {
            Ok(self
                .entries
                .lock()
                .expect("host backend lock poisoned")
                .remove(&(namespace.to_owned(), key.to_owned()))
                .is_some())
        }

        fn list_keys(&self, namespace: &str) -> Result<Vec<String>, String> {
            let mut keys: Vec<String> = self
                .entries
                .lock()
                .expect("host backend lock poisoned")
                .keys()
                .filter(|(candidate_namespace, _)| candidate_namespace == namespace)
                .map(|(_, key)| key.clone())
                .collect();
            keys.sort();
            Ok(keys)
        }

        fn clear_namespace(&self, namespace: &str) -> Result<usize, String> {
            let mut entries = self.entries.lock().expect("host backend lock poisoned");
            let initial_len = entries.len();
            entries.retain(|(candidate_namespace, _), _| candidate_namespace != namespace);
            Ok(initial_len.saturating_sub(entries.len()))
        }
    }

    #[derive(Debug)]
    struct FailingHostBackend;

    impl StorageHostBackend for FailingHostBackend {
        fn set(&self, _namespace: &str, _key: &str, _value: &[u8]) -> Result<(), String> {
            Err("simulated host backend set failure".to_owned())
        }

        fn get(&self, _namespace: &str, _key: &str) -> Result<Option<Vec<u8>>, String> {
            Err("simulated host backend get failure".to_owned())
        }

        fn delete(&self, _namespace: &str, _key: &str) -> Result<bool, String> {
            Err("simulated host backend delete failure".to_owned())
        }

        fn list_keys(&self, _namespace: &str) -> Result<Vec<String>, String> {
            Err("simulated host backend list failure".to_owned())
        }

        fn clear_namespace(&self, _namespace: &str) -> Result<usize, String> {
            Err("simulated host backend clear failure".to_owned())
        }
    }

    fn storage_cap_with_defaults() -> BrowserStorageIoCap {
        BrowserStorageIoCap::new(
            StorageAuthority::deny_all()
                .grant_backend(StorageBackend::IndexedDb)
                .grant_backend(StorageBackend::LocalStorage)
                .grant_namespace("cache:*")
                .grant_namespace("prefs:*")
                .grant_operation(StorageOperation::Get)
                .grant_operation(StorageOperation::Set)
                .grant_operation(StorageOperation::Delete)
                .grant_operation(StorageOperation::ListKeys)
                .grant_operation(StorageOperation::ClearNamespace),
            StorageQuotaPolicy {
                max_total_bytes: 256,
                max_key_bytes: 64,
                max_value_bytes: 128,
                max_namespace_bytes: 32,
                max_entries: 16,
            },
            StorageConsistencyPolicy::ImmediateReadAfterWrite,
            StorageRedactionPolicy::default(),
        )
    }

    #[test]
    fn adapter_round_trip_set_get_delete_is_deterministic() {
        let mut adapter = BrowserStorageAdapter::new(storage_cap_with_defaults());
        adapter
            .set(
                StorageBackend::IndexedDb,
                "cache:user:42",
                "profile",
                b"v1".to_vec(),
            )
            .expect("set should succeed");
        adapter
            .set(
                StorageBackend::IndexedDb,
                "cache:user:42",
                "access_token",
                b"t-1".to_vec(),
            )
            .expect("set should succeed");

        let keys = adapter
            .list_keys(StorageBackend::IndexedDb, "cache:user:42")
            .expect("list should succeed");
        assert_eq!(keys, vec!["access_token".to_owned(), "profile".to_owned()]);

        let value = adapter
            .get(StorageBackend::IndexedDb, "cache:user:42", "profile")
            .expect("get should succeed");
        assert_eq!(value, Some(b"v1".to_vec()));

        let removed = adapter
            .delete(StorageBackend::IndexedDb, "cache:user:42", "profile")
            .expect("delete should succeed");
        assert!(removed);
        assert_eq!(
            adapter
                .get(StorageBackend::IndexedDb, "cache:user:42", "profile")
                .expect("get should succeed"),
            None
        );
    }

    #[test]
    fn adapter_enforces_total_quota() {
        let cap = BrowserStorageIoCap::new(
            StorageAuthority::deny_all()
                .grant_backend(StorageBackend::LocalStorage)
                .grant_namespace("prefs:*")
                .grant_operation(StorageOperation::Set),
            StorageQuotaPolicy {
                max_total_bytes: 16,
                max_key_bytes: 16,
                max_value_bytes: 16,
                max_namespace_bytes: 16,
                max_entries: 8,
            },
            StorageConsistencyPolicy::ImmediateReadAfterWrite,
            StorageRedactionPolicy::default(),
        );
        let mut adapter = BrowserStorageAdapter::new(cap);

        adapter
            .set(
                StorageBackend::LocalStorage,
                "prefs:v1",
                "a",
                b"12".to_vec(),
            )
            .expect("first set should fit quota");

        let result = adapter.set(
            StorageBackend::LocalStorage,
            "prefs:v1",
            "abc",
            b"123456789".to_vec(),
        );
        assert!(matches!(
            result,
            Err(BrowserStorageError::Policy(
                StoragePolicyError::QuotaExceeded { .. }
            ))
        ));
    }

    #[test]
    fn adapter_denies_ungranted_namespace() {
        let mut adapter = BrowserStorageAdapter::new(storage_cap_with_defaults());
        let result = adapter.set(
            StorageBackend::IndexedDb,
            "session:v1",
            "token",
            b"x".to_vec(),
        );
        assert_eq!(
            result,
            Err(BrowserStorageError::Policy(
                StoragePolicyError::NamespaceDenied("session:v1".to_owned())
            ))
        );
    }

    #[test]
    fn adapter_records_redacted_events_when_configured() {
        let cap = BrowserStorageIoCap::new(
            StorageAuthority::deny_all()
                .grant_backend(StorageBackend::IndexedDb)
                .grant_namespace("cache:*")
                .grant_operation(StorageOperation::Set),
            StorageQuotaPolicy::default(),
            StorageConsistencyPolicy::ImmediateReadAfterWrite,
            StorageRedactionPolicy {
                redact_keys: true,
                redact_namespaces: true,
                redact_value_lengths: true,
            },
        );
        let mut adapter = BrowserStorageAdapter::new(cap);

        let result = adapter.set(
            StorageBackend::IndexedDb,
            "cache:user:9001",
            "secret-key",
            b"payload".to_vec(),
        );
        assert!(result.is_ok());

        let event = adapter.events().last().expect("event should exist");
        assert_eq!(event.outcome, StorageEventOutcome::Allowed);
        assert_eq!(event.reason_code, StorageEventReasonCode::Allowed);
        assert_eq!(event.namespace_label, "namespace[len:15]");
        assert_eq!(event.key_label.as_deref(), Some("key[len:10]"));
        assert_eq!(event.value_len, None);
    }

    #[test]
    fn adapter_records_denied_reason_code_for_policy_error() {
        let mut adapter = BrowserStorageAdapter::new(storage_cap_with_defaults());
        let result = adapter.clear_namespace(StorageBackend::IndexedDb, "session:v1");
        assert_eq!(
            result,
            Err(BrowserStorageError::Policy(
                StoragePolicyError::NamespaceDenied("session:v1".to_owned())
            ))
        );

        let event = adapter.events().last().expect("event should exist");
        assert_eq!(event.outcome, StorageEventOutcome::Denied);
        assert_eq!(event.reason_code, StorageEventReasonCode::NamespaceDenied);
    }

    #[test]
    fn adapter_backend_unavailable_is_deterministic_and_traced() {
        let mut adapter = BrowserStorageAdapter::new(storage_cap_with_defaults());
        adapter.set_backend_available(StorageBackend::IndexedDb, false);

        let result = adapter.set(
            StorageBackend::IndexedDb,
            "cache:user:1",
            "token",
            b"abc".to_vec(),
        );
        assert_eq!(
            result,
            Err(BrowserStorageError::BackendUnavailable(
                StorageBackend::IndexedDb
            ))
        );

        let event = adapter.events().last().expect("event should exist");
        assert_eq!(event.outcome, StorageEventOutcome::Denied);
        assert_eq!(
            event.reason_code,
            StorageEventReasonCode::BackendUnavailable
        );
    }

    #[test]
    fn adapter_eventual_list_is_stale_then_converges() {
        let cap = BrowserStorageIoCap::new(
            StorageAuthority::deny_all()
                .grant_backend(StorageBackend::IndexedDb)
                .grant_namespace("cache:*")
                .grant_operation(StorageOperation::Get)
                .grant_operation(StorageOperation::Set)
                .grant_operation(StorageOperation::Delete)
                .grant_operation(StorageOperation::ListKeys),
            StorageQuotaPolicy::default(),
            StorageConsistencyPolicy::ReadAfterWriteEventualList,
            StorageRedactionPolicy::default(),
        );
        let mut adapter = BrowserStorageAdapter::new(cap);

        adapter
            .set(
                StorageBackend::IndexedDb,
                "cache:user:7",
                "profile",
                b"v1".to_vec(),
            )
            .expect("set should succeed");
        assert_eq!(
            adapter
                .get(StorageBackend::IndexedDb, "cache:user:7", "profile")
                .expect("get should succeed"),
            Some(b"v1".to_vec())
        );

        assert_eq!(
            adapter
                .list_keys(StorageBackend::IndexedDb, "cache:user:7")
                .expect("first list should succeed"),
            Vec::<String>::new()
        );
        assert_eq!(
            adapter
                .list_keys(StorageBackend::IndexedDb, "cache:user:7")
                .expect("second list should converge"),
            vec!["profile".to_owned()]
        );

        adapter
            .delete(StorageBackend::IndexedDb, "cache:user:7", "profile")
            .expect("delete should succeed");
        assert_eq!(
            adapter
                .get(StorageBackend::IndexedDb, "cache:user:7", "profile")
                .expect("get should succeed"),
            None
        );
        assert_eq!(
            adapter
                .list_keys(StorageBackend::IndexedDb, "cache:user:7")
                .expect("first post-delete list should be stale"),
            vec!["profile".to_owned()]
        );
        assert_eq!(
            adapter
                .list_keys(StorageBackend::IndexedDb, "cache:user:7")
                .expect("second post-delete list should converge"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn adapter_flush_namespace_list_view_forces_convergence() {
        let cap = BrowserStorageIoCap::new(
            StorageAuthority::deny_all()
                .grant_backend(StorageBackend::IndexedDb)
                .grant_namespace("cache:*")
                .grant_operation(StorageOperation::ListKeys)
                .grant_operation(StorageOperation::Set),
            StorageQuotaPolicy::default(),
            StorageConsistencyPolicy::ReadAfterWriteEventualList,
            StorageRedactionPolicy::default(),
        );
        let mut adapter = BrowserStorageAdapter::new(cap);
        adapter
            .set(
                StorageBackend::IndexedDb,
                "cache:user:9",
                "profile",
                b"v2".to_vec(),
            )
            .expect("set should succeed");

        adapter.flush_namespace_list_view(StorageBackend::IndexedDb, "cache:user:9");
        assert_eq!(
            adapter
                .list_keys(StorageBackend::IndexedDb, "cache:user:9")
                .expect("list should succeed"),
            vec!["profile".to_owned()]
        );
    }

    #[test]
    fn adapter_clear_namespace_updates_used_bytes() {
        let mut adapter = BrowserStorageAdapter::new(storage_cap_with_defaults());
        adapter
            .set(
                StorageBackend::IndexedDb,
                "cache:user:42",
                "a",
                b"12".to_vec(),
            )
            .expect("set should succeed");
        adapter
            .set(
                StorageBackend::IndexedDb,
                "cache:user:42",
                "b",
                b"123".to_vec(),
            )
            .expect("set should succeed");
        assert!(adapter.used_bytes() > 0);

        let removed = adapter
            .clear_namespace(StorageBackend::IndexedDb, "cache:user:42")
            .expect("clear should succeed");
        assert_eq!(removed, 2);
        assert_eq!(adapter.entry_count(), 0);
        assert_eq!(adapter.used_bytes(), 0);
    }

    #[test]
    fn adapter_routes_local_storage_operations_through_registered_host_backend() {
        let host_backend = Arc::new(MockHostBackend::default());
        let mut adapter = BrowserStorageAdapter::new(storage_cap_with_defaults());
        adapter.register_host_backend(StorageBackend::LocalStorage, host_backend.clone());

        adapter
            .set(
                StorageBackend::LocalStorage,
                "prefs:v1",
                "theme",
                b"dark".to_vec(),
            )
            .expect("host-backed set should succeed");
        assert_eq!(
            host_backend
                .get("prefs:v1", "theme")
                .expect("host-backed get should succeed"),
            Some(b"dark".to_vec())
        );

        let listed = adapter
            .list_keys(StorageBackend::LocalStorage, "prefs:v1")
            .expect("host-backed list should succeed");
        assert_eq!(listed, vec!["theme".to_owned()]);

        let removed = adapter
            .delete(StorageBackend::LocalStorage, "prefs:v1", "theme")
            .expect("host-backed delete should succeed");
        assert!(removed);
        assert_eq!(
            host_backend
                .get("prefs:v1", "theme")
                .expect("host-backed get should succeed"),
            None
        );
    }

    #[test]
    fn adapter_records_host_backend_failures_with_deterministic_reason_code() {
        let mut adapter = BrowserStorageAdapter::new(storage_cap_with_defaults());
        adapter.register_host_backend(StorageBackend::LocalStorage, Arc::new(FailingHostBackend));

        let result = adapter.set(
            StorageBackend::LocalStorage,
            "prefs:v1",
            "theme",
            b"light".to_vec(),
        );
        assert!(matches!(
            result,
            Err(BrowserStorageError::HostBackend {
                backend: StorageBackend::LocalStorage,
                operation: StorageOperation::Set,
                ..
            })
        ));

        let event = adapter.events().last().expect("event should exist");
        assert_eq!(event.outcome, StorageEventOutcome::Denied);
        assert_eq!(event.reason_code, StorageEventReasonCode::HostBackendError);
    }
}
