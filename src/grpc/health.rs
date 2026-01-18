//! gRPC Health Checking Protocol implementation.
//!
//! Implements the standard gRPC health checking protocol as defined in
//! [grpc/grpc-proto](https://github.com/grpc/grpc-proto/blob/master/grpc/health/v1/health.proto).
//!
//! # Example
//!
//! ```ignore
//! use asupersync::grpc::health::{HealthService, ServingStatus};
//!
//! // Create health service
//! let health = HealthService::new();
//!
//! // Set service status
//! health.set_status("my.service.Name", ServingStatus::Serving);
//!
//! // Register with gRPC server
//! let server = Server::builder()
//!     .add_service(health.clone())
//!     .build();
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use super::service::{NamedService, ServiceDescriptor, ServiceHandler};
use super::status::Status;
use super::streaming::{Request, Response};

/// Service status for health checking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ServingStatus {
    /// Status is unknown.
    Unknown = 0,
    /// Service is healthy and serving requests.
    Serving = 1,
    /// Service is not serving requests.
    NotServing = 2,
    /// Used only by Watch. Indicates the service is in a transient state.
    ServiceUnknown = 3,
}

impl ServingStatus {
    /// Returns true if the service is healthy.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Serving)
    }

    /// Convert from i32.
    #[must_use]
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unknown),
            1 => Some(Self::Serving),
            2 => Some(Self::NotServing),
            3 => Some(Self::ServiceUnknown),
            _ => None,
        }
    }
}

impl Default for ServingStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Display for ServingStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "UNKNOWN"),
            Self::Serving => write!(f, "SERVING"),
            Self::NotServing => write!(f, "NOT_SERVING"),
            Self::ServiceUnknown => write!(f, "SERVICE_UNKNOWN"),
        }
    }
}

/// Request for health check.
#[derive(Debug, Clone, Default)]
pub struct HealthCheckRequest {
    /// The service name to check.
    ///
    /// Empty string means check the overall server health.
    pub service: String,
}

impl HealthCheckRequest {
    /// Create a new request for a specific service.
    #[must_use]
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
        }
    }

    /// Create a request for overall server health.
    #[must_use]
    pub fn server() -> Self {
        Self::default()
    }
}

/// Response from health check.
#[derive(Debug, Clone)]
pub struct HealthCheckResponse {
    /// The serving status.
    pub status: ServingStatus,
}

impl HealthCheckResponse {
    /// Create a new response.
    #[must_use]
    pub fn new(status: ServingStatus) -> Self {
        Self { status }
    }
}

impl Default for HealthCheckResponse {
    fn default() -> Self {
        Self {
            status: ServingStatus::Unknown,
        }
    }
}

/// Health checking service.
///
/// This service implements the gRPC Health Checking Protocol, allowing
/// clients to query the health status of services.
///
/// # Thread Safety
///
/// The service is thread-safe and can be cloned to share between handlers.
#[derive(Debug, Clone)]
pub struct HealthService {
    /// Service statuses.
    statuses: Arc<RwLock<HashMap<String, ServingStatus>>>,
}

impl HealthService {
    /// Create a new health service.
    #[must_use]
    pub fn new() -> Self {
        Self {
            statuses: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set the status of a service.
    ///
    /// Use an empty string for the overall server status.
    pub fn set_status(&self, service: impl Into<String>, status: ServingStatus) {
        let mut statuses = self.statuses.write().expect("lock poisoned");
        statuses.insert(service.into(), status);
    }

    /// Set the status of the overall server.
    pub fn set_server_status(&self, status: ServingStatus) {
        self.set_status("", status);
    }

    /// Get the status of a service.
    ///
    /// Returns `None` if the service is not registered.
    #[must_use]
    pub fn get_status(&self, service: &str) -> Option<ServingStatus> {
        let statuses = self.statuses.read().expect("lock poisoned");
        statuses.get(service).copied()
    }

    /// Check if a service is serving.
    #[must_use]
    pub fn is_serving(&self, service: &str) -> bool {
        self.get_status(service)
            .map(|s| s.is_healthy())
            .unwrap_or(false)
    }

    /// Clear all service statuses.
    pub fn clear(&self) {
        let mut statuses = self.statuses.write().expect("lock poisoned");
        statuses.clear();
    }

    /// Remove a service from health tracking.
    pub fn clear_status(&self, service: &str) {
        let mut statuses = self.statuses.write().expect("lock poisoned");
        statuses.remove(service);
    }

    /// Get all registered services.
    #[must_use]
    pub fn services(&self) -> Vec<String> {
        let statuses = self.statuses.read().expect("lock poisoned");
        statuses.keys().cloned().collect()
    }

    /// Handle a health check request.
    pub fn check(&self, request: &HealthCheckRequest) -> Result<HealthCheckResponse, Status> {
        let statuses = self.statuses.read().expect("lock poisoned");

        if let Some(&status) = statuses.get(&request.service) {
            Ok(HealthCheckResponse::new(status))
        } else if request.service.is_empty() {
            // No explicit server status set, default to SERVING if any services are registered
            if statuses.is_empty() {
                Ok(HealthCheckResponse::new(ServingStatus::ServiceUnknown))
            } else {
                // Check if all services are healthy
                let all_healthy = statuses.values().all(|s| s.is_healthy());
                if all_healthy {
                    Ok(HealthCheckResponse::new(ServingStatus::Serving))
                } else {
                    Ok(HealthCheckResponse::new(ServingStatus::NotServing))
                }
            }
        } else {
            Err(Status::not_found(format!(
                "service '{}' not registered for health checking",
                request.service
            )))
        }
    }

    /// Async check handler for use with gRPC server.
    pub fn check_async(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<HealthCheckResponse>, Status>> + Send>>
    {
        let result = self.check(request.get_ref());
        Box::pin(async move { result.map(Response::new) })
    }
}

impl Default for HealthService {
    fn default() -> Self {
        Self::new()
    }
}

impl NamedService for HealthService {
    const NAME: &'static str = "grpc.health.v1.Health";
}

impl ServiceHandler for HealthService {
    fn descriptor(&self) -> &ServiceDescriptor {
        static METHODS: &[super::service::MethodDescriptor] = &[
            super::service::MethodDescriptor::unary("Check", "/grpc.health.v1.Health/Check"),
            super::service::MethodDescriptor::server_streaming(
                "Watch",
                "/grpc.health.v1.Health/Watch",
            ),
        ];
        static DESC: ServiceDescriptor = ServiceDescriptor::new("Health", "grpc.health.v1", METHODS);
        &DESC
    }

    fn method_names(&self) -> Vec<&str> {
        vec!["Check", "Watch"]
    }
}

/// Health reporter for tracking service lifecycle.
///
/// Provides a convenient way to manage health status during service
/// initialization and shutdown.
#[derive(Debug)]
pub struct HealthReporter {
    service: HealthService,
    service_name: String,
}

impl HealthReporter {
    /// Create a new health reporter for a service.
    #[must_use]
    pub fn new(service: HealthService, service_name: impl Into<String>) -> Self {
        Self {
            service,
            service_name: service_name.into(),
        }
    }

    /// Mark the service as serving.
    pub fn set_serving(&self) {
        self.service.set_status(&self.service_name, ServingStatus::Serving);
    }

    /// Mark the service as not serving.
    pub fn set_not_serving(&self) {
        self.service.set_status(&self.service_name, ServingStatus::NotServing);
    }

    /// Get the current status.
    #[must_use]
    pub fn status(&self) -> ServingStatus {
        self.service
            .get_status(&self.service_name)
            .unwrap_or(ServingStatus::Unknown)
    }
}

impl Drop for HealthReporter {
    fn drop(&mut self) {
        // Clear the service status when the reporter is dropped
        self.service.clear_status(&self.service_name);
    }
}

/// Builder for creating health services with initial statuses.
#[derive(Debug, Default)]
pub struct HealthServiceBuilder {
    statuses: HashMap<String, ServingStatus>,
}

impl HealthServiceBuilder {
    /// Create a new builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a service with a status.
    #[must_use]
    pub fn add(mut self, service: impl Into<String>, status: ServingStatus) -> Self {
        self.statuses.insert(service.into(), status);
        self
    }

    /// Add multiple services all set to SERVING.
    #[must_use]
    pub fn add_serving(mut self, services: impl IntoIterator<Item = impl Into<String>>) -> Self {
        for service in services {
            self.statuses.insert(service.into(), ServingStatus::Serving);
        }
        self
    }

    /// Build the health service.
    #[must_use]
    pub fn build(self) -> HealthService {
        let service = HealthService::new();
        for (name, status) in self.statuses {
            service.set_status(name, status);
        }
        service
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serving_status_from_i32() {
        assert_eq!(ServingStatus::from_i32(0), Some(ServingStatus::Unknown));
        assert_eq!(ServingStatus::from_i32(1), Some(ServingStatus::Serving));
        assert_eq!(ServingStatus::from_i32(2), Some(ServingStatus::NotServing));
        assert_eq!(ServingStatus::from_i32(3), Some(ServingStatus::ServiceUnknown));
        assert_eq!(ServingStatus::from_i32(4), None);
    }

    #[test]
    fn serving_status_is_healthy() {
        assert!(!ServingStatus::Unknown.is_healthy());
        assert!(ServingStatus::Serving.is_healthy());
        assert!(!ServingStatus::NotServing.is_healthy());
        assert!(!ServingStatus::ServiceUnknown.is_healthy());
    }

    #[test]
    fn serving_status_display() {
        assert_eq!(ServingStatus::Serving.to_string(), "SERVING");
        assert_eq!(ServingStatus::NotServing.to_string(), "NOT_SERVING");
    }

    #[test]
    fn health_service_set_and_get() {
        let service = HealthService::new();

        service.set_status("test.Service", ServingStatus::Serving);
        assert_eq!(
            service.get_status("test.Service"),
            Some(ServingStatus::Serving)
        );

        service.set_status("test.Service", ServingStatus::NotServing);
        assert_eq!(
            service.get_status("test.Service"),
            Some(ServingStatus::NotServing)
        );
    }

    #[test]
    fn health_service_is_serving() {
        let service = HealthService::new();

        assert!(!service.is_serving("unknown"));

        service.set_status("test", ServingStatus::Serving);
        assert!(service.is_serving("test"));

        service.set_status("test", ServingStatus::NotServing);
        assert!(!service.is_serving("test"));
    }

    #[test]
    fn health_service_check() {
        let service = HealthService::new();
        service.set_status("test.Service", ServingStatus::Serving);

        let req = HealthCheckRequest::new("test.Service");
        let resp = service.check(&req).unwrap();
        assert_eq!(resp.status, ServingStatus::Serving);

        let req = HealthCheckRequest::new("unknown.Service");
        let err = service.check(&req).unwrap_err();
        assert_eq!(err.code(), super::super::status::Code::NotFound);
    }

    #[test]
    fn health_service_server_status() {
        let service = HealthService::new();

        // No services registered
        let req = HealthCheckRequest::server();
        let resp = service.check(&req).unwrap();
        assert_eq!(resp.status, ServingStatus::ServiceUnknown);

        // Add a healthy service
        service.set_status("test", ServingStatus::Serving);
        let resp = service.check(&req).unwrap();
        assert_eq!(resp.status, ServingStatus::Serving);

        // Add an unhealthy service
        service.set_status("test2", ServingStatus::NotServing);
        let resp = service.check(&req).unwrap();
        assert_eq!(resp.status, ServingStatus::NotServing);

        // Explicit server status overrides
        service.set_server_status(ServingStatus::Serving);
        let resp = service.check(&req).unwrap();
        assert_eq!(resp.status, ServingStatus::Serving);
    }

    #[test]
    fn health_service_clear() {
        let service = HealthService::new();
        service.set_status("a", ServingStatus::Serving);
        service.set_status("b", ServingStatus::Serving);

        service.clear_status("a");
        assert!(service.get_status("a").is_none());
        assert!(service.get_status("b").is_some());

        service.clear();
        assert!(service.get_status("b").is_none());
    }

    #[test]
    fn health_service_services() {
        let service = HealthService::new();
        service.set_status("a", ServingStatus::Serving);
        service.set_status("b", ServingStatus::NotServing);

        let services = service.services();
        assert!(services.contains(&"a".to_string()));
        assert!(services.contains(&"b".to_string()));
    }

    #[test]
    fn health_reporter() {
        let service = HealthService::new();
        {
            let reporter = HealthReporter::new(service.clone(), "my.Service");
            reporter.set_serving();
            assert_eq!(reporter.status(), ServingStatus::Serving);
            assert!(service.is_serving("my.Service"));
        }
        // Service status cleared on drop
        assert!(service.get_status("my.Service").is_none());
    }

    #[test]
    fn health_service_builder() {
        let service = HealthServiceBuilder::new()
            .add("explicit", ServingStatus::NotServing)
            .add_serving(["a", "b", "c"])
            .build();

        assert_eq!(service.get_status("explicit"), Some(ServingStatus::NotServing));
        assert_eq!(service.get_status("a"), Some(ServingStatus::Serving));
        assert_eq!(service.get_status("b"), Some(ServingStatus::Serving));
        assert_eq!(service.get_status("c"), Some(ServingStatus::Serving));
    }

    #[test]
    fn health_service_named_service() {
        assert_eq!(HealthService::NAME, "grpc.health.v1.Health");
    }

    #[test]
    fn health_service_descriptor() {
        let service = HealthService::new();
        let desc = service.descriptor();
        assert_eq!(desc.name, "Health");
        assert_eq!(desc.package, "grpc.health.v1");
        assert_eq!(desc.methods.len(), 2);
    }

    #[test]
    fn health_service_method_names() {
        let service = HealthService::new();
        let names = service.method_names();
        assert!(names.contains(&"Check"));
        assert!(names.contains(&"Watch"));
    }

    #[test]
    fn health_check_request_constructors() {
        let req = HealthCheckRequest::new("my.Service");
        assert_eq!(req.service, "my.Service");

        let req = HealthCheckRequest::server();
        assert_eq!(req.service, "");
    }

    #[test]
    fn health_service_clone() {
        let service1 = HealthService::new();
        let service2 = service1.clone();

        service1.set_status("test", ServingStatus::Serving);
        assert_eq!(service2.get_status("test"), Some(ServingStatus::Serving));
    }
}
