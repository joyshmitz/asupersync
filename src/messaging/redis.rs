//! Redis client with RESP protocol and Cx integration.
//!
//! This module provides a pure Rust Redis client implementing the RESP
//! (REdis Serialization Protocol) with Cx integration for cancel-correct
//! command execution.

use crate::cx::Cx;
use std::fmt;
use std::io;
use std::time::Duration;

/// Error type for Redis operations.
#[derive(Debug)]
pub enum RedisError {
    /// I/O error during communication.
    Io(io::Error),
    /// Protocol error (malformed RESP response).
    Protocol(String),
    /// Redis returned an error response.
    Redis(String),
    /// Connection pool exhausted.
    PoolExhausted,
    /// Invalid URL format.
    InvalidUrl(String),
    /// Operation cancelled.
    Cancelled,
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "Redis I/O error: {e}"),
            Self::Protocol(msg) => write!(f, "Redis protocol error: {msg}"),
            Self::Redis(msg) => write!(f, "Redis error: {msg}"),
            Self::PoolExhausted => write!(f, "Redis connection pool exhausted"),
            Self::InvalidUrl(url) => write!(f, "Invalid Redis URL: {url}"),
            Self::Cancelled => write!(f, "Redis operation cancelled"),
        }
    }
}

impl std::error::Error for RedisError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for RedisError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// RESP (REdis Serialization Protocol) value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// Simple string (prefixed with +).
    SimpleString(String),
    /// Error message (prefixed with -).
    Error(String),
    /// 64-bit signed integer (prefixed with :).
    Integer(i64),
    /// Bulk string (prefixed with $, can be null).
    BulkString(Option<Vec<u8>>),
    /// Array of RESP values (prefixed with *, can be null).
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    /// Encode this value to RESP wire format.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf);
        buf
    }

    /// Encode this value into an existing buffer.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        match self {
            Self::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::Error(e) => {
                buf.push(b'-');
                buf.extend_from_slice(e.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::Integer(i) => {
                buf.push(b':');
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::BulkString(Some(data)) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Self::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            Self::Array(Some(arr)) => {
                buf.push(b'*');
                buf.extend_from_slice(arr.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in arr {
                    item.encode_into(buf);
                }
            }
            Self::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
        }
    }

    /// Try to extract as a bulk string (bytes).
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::BulkString(Some(b)) => Some(b),
            _ => None,
        }
    }

    /// Try to extract as an integer.
    #[must_use]
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Check if this is an OK response.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::SimpleString(s) if s == "OK")
    }
}

/// Configuration for Redis client.
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Host address.
    pub host: String,
    /// Port.
    pub port: u16,
    /// Database index.
    pub database: u8,
    /// Password for AUTH.
    pub password: Option<String>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            database: 0,
            password: None,
        }
    }
}

impl RedisConfig {
    /// Create config from a Redis URL.
    pub fn from_url(url: &str) -> Result<Self, RedisError> {
        let url = url
            .strip_prefix("redis://")
            .ok_or_else(|| RedisError::InvalidUrl(url.to_string()))?;

        let mut config = Self::default();

        let url = if let Some((password, rest)) = url.split_once('@') {
            config.password = Some(password.to_string());
            rest
        } else {
            url
        };

        let (host_port, database) = if let Some((hp, db)) = url.split_once('/') {
            (hp, Some(db))
        } else {
            (url, None)
        };

        if let Some((host, port)) = host_port.split_once(':') {
            config.host = host.to_string();
            config.port = port
                .parse()
                .map_err(|_| RedisError::InvalidUrl(format!("invalid port: {port}")))?;
        } else if !host_port.is_empty() {
            config.host = host_port.to_string();
        }

        if let Some(db) = database {
            if !db.is_empty() {
                config.database = db
                    .parse()
                    .map_err(|_| RedisError::InvalidUrl(format!("invalid database: {db}")))?;
            }
        }

        Ok(config)
    }
}

/// Redis client (Phase 0 stub).
#[derive(Debug)]
pub struct RedisClient {
    config: RedisConfig,
}

impl RedisClient {
    /// Connect to Redis.
    pub async fn connect(_cx: &Cx, url: &str) -> Result<Self, RedisError> {
        let config = RedisConfig::from_url(url)?;
        Ok(Self { config })
    }

    /// Execute a raw command (Phase 0 stub).
    #[allow(unused_variables)]
    pub async fn cmd(&self, cx: &Cx, args: &[&str]) -> Result<RespValue, RedisError> {
        cx.checkpoint().map_err(|_| RedisError::Cancelled)?;
        Err(RedisError::Io(io::Error::new(
            io::ErrorKind::Other,
            "Phase 0: requires reactor integration",
        )))
    }

    /// GET key.
    pub async fn get(&self, cx: &Cx, key: &str) -> Result<Option<Vec<u8>>, RedisError> {
        let response = self.cmd(cx, &["GET", key]).await?;
        Ok(response.as_bytes().map(|b| b.to_vec()))
    }

    /// SET key value.
    pub async fn set(
        &self,
        cx: &Cx,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), RedisError> {
        let value_str = String::from_utf8_lossy(value);
        if let Some(ttl) = ttl {
            self.cmd(cx, &["SET", key, &value_str, "EX", &ttl.as_secs().to_string()]).await?;
        } else {
            self.cmd(cx, &["SET", key, &value_str]).await?;
        }
        Ok(())
    }

    /// INCR key.
    pub async fn incr(&self, cx: &Cx, key: &str) -> Result<i64, RedisError> {
        let response = self.cmd(cx, &["INCR", key]).await?;
        response
            .as_integer()
            .ok_or_else(|| RedisError::Protocol("INCR did not return integer".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resp_encode_simple_string() {
        let value = RespValue::SimpleString("OK".to_string());
        assert_eq!(value.encode(), b"+OK\r\n");
    }

    #[test]
    fn test_resp_encode_integer() {
        let value = RespValue::Integer(42);
        assert_eq!(value.encode(), b":42\r\n");
    }

    #[test]
    fn test_config_from_url() {
        let config = RedisConfig::from_url("redis://localhost:6379").unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 6379);
    }
}
