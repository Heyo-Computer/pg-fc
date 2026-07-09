//! Shared axum application state: the pooler registry plus dashboard settings.

use std::sync::Arc;

use crate::config::DashboardConfig;
use crate::registry::SchemaRegistry;

/// Cloned per request by axum; both fields are `Arc` so cloning is cheap.
#[derive(Clone)]
pub struct DashState {
    pub registry: Arc<SchemaRegistry>,
    pub cfg: Arc<DashboardConfig>,
}
