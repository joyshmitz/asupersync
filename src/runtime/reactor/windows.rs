//! Windows IOCP reactor implementation.
//!
//! On Windows, the reactor is built on I/O Completion Ports (IOCP), which are
//! completion-based rather than readiness-based. The production implementation
//! will adapt completion signals into readiness events expected by the runtime.

use super::{Events, Interest, Reactor, Source, Token};

// Windows implementation placeholder.
#[cfg(target_os = "windows")]
mod iocp_impl {
    use super::{Events, Interest, Reactor, Source, Token};
    use std::io;
    use std::time::Duration;

    /// IOCP-based reactor (Windows).
    #[derive(Debug, Default)]
    pub struct IocpReactor;

    impl IocpReactor {
        /// Create a new IOCP reactor.
        ///
        /// # Errors
        ///
        /// Returns `Unsupported` until the IOCP backend is implemented.
        pub fn new() -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is not implemented yet",
            ))
        }
    }

    impl Reactor for IocpReactor {
        fn register(
            &self,
            _source: &dyn Source,
            _token: Token,
            _interest: Interest,
        ) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is not implemented yet",
            ))
        }

        fn modify(&self, _token: Token, _interest: Interest) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is not implemented yet",
            ))
        }

        fn deregister(&self, _token: Token) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is not implemented yet",
            ))
        }

        fn poll(&self, _events: &mut Events, _timeout: Option<Duration>) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is not implemented yet",
            ))
        }

        fn wake(&self) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is not implemented yet",
            ))
        }

        fn registration_count(&self) -> usize {
            0
        }
    }
}

// Stub for non-Windows platforms (keeps docs/builds consistent).
#[cfg(not(target_os = "windows"))]
mod stub {
    use super::{Events, Interest, Reactor, Source, Token};
    use std::io;
    use std::time::Duration;

    /// IOCP-based reactor (Windows-only).
    #[derive(Debug, Default)]
    pub struct IocpReactor;

    impl IocpReactor {
        /// Create a new IOCP reactor.
        ///
        /// # Errors
        ///
        /// Always returns `Unsupported` on non-Windows platforms.
        pub fn new() -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is only available on Windows",
            ))
        }
    }

    impl Reactor for IocpReactor {
        fn register(
            &self,
            _source: &dyn Source,
            _token: Token,
            _interest: Interest,
        ) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is only available on Windows",
            ))
        }

        fn modify(&self, _token: Token, _interest: Interest) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is only available on Windows",
            ))
        }

        fn deregister(&self, _token: Token) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is only available on Windows",
            ))
        }

        fn poll(&self, _events: &mut Events, _timeout: Option<Duration>) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is only available on Windows",
            ))
        }

        fn wake(&self) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IocpReactor is only available on Windows",
            ))
        }

        fn registration_count(&self) -> usize {
            0
        }
    }
}

#[cfg(target_os = "windows")]
pub use iocp_impl::IocpReactor;

#[cfg(not(target_os = "windows"))]
pub use stub::IocpReactor;
