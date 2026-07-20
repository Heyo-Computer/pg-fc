//! Shared axum application state: the pooler registry plus dashboard settings.

use std::sync::Arc;

use crate::config::DashboardConfig;
use crate::registry::SchemaRegistry;

use super::alerts::AlertStore;

/// Cloned per request by axum; every field is an `Arc` so cloning is cheap. The
/// shared `alerts` store is read by the monitoring page and mutated by its
/// add/delete actions and the background evaluator.
#[derive(Clone)]
pub struct DashState {
    pub registry: Arc<SchemaRegistry>,
    pub cfg: Arc<DashboardConfig>,
    pub alerts: Arc<AlertStore>,
}
