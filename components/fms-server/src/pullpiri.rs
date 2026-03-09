// SPDX-FileCopyrightText: 2023 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! PULLPIRI REST API handlers
//!
//! Provides REST API endpoints for PULLPIRI status, workloads, and scenarios.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

use crate::influx_reader::InfluxReader;
use crate::pullpiri_publisher::ScenarioPublisher;

/// Application state shared across handlers
pub struct AppState {
    pub influx_reader: Arc<InfluxReader>,
    pub scenario_publisher: Option<Arc<ScenarioPublisher>>,
}

// ============================================================================
// Response Models
// ============================================================================

/// PULLPIRI status response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullpiriStatusResponse {
    /// List of vehicle statuses
    pub statuses: Vec<VehiclePullpiriStatus>,
    /// Request timestamp
    pub request_server_date_time: DateTime<Utc>,
}

/// PULLPIRI status for a single vehicle
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VehiclePullpiriStatus {
    /// Vehicle ID (VIN)
    pub vehicle_id: String,
    /// Timestamp of the status
    pub timestamp: DateTime<Utc>,
    /// Number of workloads
    pub workload_count: i32,
    /// Number of scenarios
    pub scenario_count: i32,
}

/// Workload list response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadListResponse {
    /// List of workloads
    pub workloads: Vec<WorkloadInfo>,
    /// Request timestamp
    pub request_server_date_time: DateTime<Utc>,
}

/// Workload information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadInfo {
    /// Vehicle ID (VIN)
    pub vehicle_id: String,
    /// Workload name
    pub name: String,
    /// Workload state
    pub state: String,
    /// Container ID (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    /// Started timestamp (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// Finished timestamp (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Error message (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Scenario list response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioListResponse {
    /// List of scenarios
    pub scenarios: Vec<ScenarioInfo>,
    /// Request timestamp
    pub request_server_date_time: DateTime<Utc>,
}

/// Scenario information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioInfo {
    /// Vehicle ID (VIN)
    pub vehicle_id: String,
    /// Scenario name
    pub name: String,
    /// Scenario state
    pub state: String,
    /// Trigger count
    pub trigger_count: i32,
    /// Last triggered timestamp (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_triggered: Option<DateTime<Utc>>,
    /// Target workload (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_workload: Option<String>,
}

/// Scenario deployment request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioDeployRequest {
    /// Target vehicle ID (VIN)
    pub vehicle_id: String,
    /// Deployment action (deploy, undeploy, restart)
    #[serde(default = "default_action")]
    pub action: String,
}

fn default_action() -> String {
    "deploy".to_string()
}

/// Scenario deployment response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioDeployResponse {
    /// Whether the deployment was successful
    pub success: bool,
    /// Scenario ID
    pub scenario_id: String,
    /// Target vehicle ID
    pub vehicle_id: String,
    /// Deployment action
    pub action: String,
    /// Message
    pub message: String,
    /// Request timestamp
    pub request_server_date_time: DateTime<Utc>,
}

// ============================================================================
// API Handlers
// ============================================================================

/// GET /pullpiri/status
///
/// Returns the PULLPIRI status summary for all vehicles or a specific vehicle.
///
/// Query parameters:
/// - `vin`: Optional vehicle VIN to filter by
/// - `latestOnly`: If true, returns only the latest status (default: true)
pub async fn get_status(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let vin_filter = params.get("vin").cloned();
    let latest_only = params
        .get("latestOnly")
        .map(|v| v == "true")
        .unwrap_or(true);

    app_state
        .influx_reader
        .get_pullpiri_status(vin_filter.as_deref(), latest_only)
        .await
        .map(|statuses| {
            let response = PullpiriStatusResponse {
                statuses,
                request_server_date_time: Utc::now(),
            };
            Json(json!(response))
        })
        .map_err(|e| {
            error!("Error retrieving PULLPIRI status: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// GET /pullpiri/workloads
///
/// Returns the list of workloads for all vehicles or a specific vehicle.
///
/// Query parameters:
/// - `vin`: Optional vehicle VIN to filter by
/// - `state`: Optional state filter (e.g., "running", "stopped")
/// - `latestOnly`: If true, returns only the latest status (default: true)
pub async fn get_workloads(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let vin_filter = params.get("vin").cloned();
    let state_filter = params.get("state").cloned();
    let latest_only = params
        .get("latestOnly")
        .map(|v| v == "true")
        .unwrap_or(true);

    app_state
        .influx_reader
        .get_workloads(vin_filter.as_deref(), state_filter.as_deref(), latest_only)
        .await
        .map(|workloads| {
            let response = WorkloadListResponse {
                workloads,
                request_server_date_time: Utc::now(),
            };
            Json(json!(response))
        })
        .map_err(|e| {
            error!("Error retrieving workloads: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// GET /pullpiri/scenarios
///
/// Returns the list of scenarios for all vehicles or a specific vehicle.
///
/// Query parameters:
/// - `vin`: Optional vehicle VIN to filter by
/// - `state`: Optional state filter (e.g., "active", "inactive")
/// - `latestOnly`: If true, returns only the latest status (default: true)
pub async fn get_scenarios(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let vin_filter = params.get("vin").cloned();
    let state_filter = params.get("state").cloned();
    let latest_only = params
        .get("latestOnly")
        .map(|v| v == "true")
        .unwrap_or(true);

    app_state
        .influx_reader
        .get_scenarios(vin_filter.as_deref(), state_filter.as_deref(), latest_only)
        .await
        .map(|scenarios| {
            let response = ScenarioListResponse {
                scenarios,
                request_server_date_time: Utc::now(),
            };
            Json(json!(response))
        })
        .map_err(|e| {
            error!("Error retrieving scenarios: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// POST /pullpiri/scenarios/:id/deploy
///
/// Deploys a scenario to a target vehicle.
///
/// Path parameters:
/// - `id`: Scenario ID to deploy
///
/// Request body:
/// - `vehicle_id`: Target vehicle VIN
/// - `action`: Deployment action (deploy, undeploy, restart) - default: "deploy"
pub async fn deploy_scenario(
    State(app_state): State<Arc<AppState>>,
    Path(scenario_id): Path<String>,
    Json(request): Json<ScenarioDeployRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Check if scenario publisher is available
    let publisher = match &app_state.scenario_publisher {
        Some(p) => p,
        None => {
            warn!("Scenario deployment requested but publisher is not configured");
            let response = ScenarioDeployResponse {
                success: false,
                scenario_id,
                vehicle_id: request.vehicle_id,
                action: request.action,
                message: "Scenario publisher is not configured. Set ZENOH_CONFIG environment variable.".to_string(),
                request_server_date_time: Utc::now(),
            };
            return Ok(Json(json!(response)));
        }
    };

    // Validate action
    let valid_actions = ["deploy", "undeploy", "restart"];
    if !valid_actions.contains(&request.action.as_str()) {
        let response = ScenarioDeployResponse {
            success: false,
            scenario_id,
            vehicle_id: request.vehicle_id,
            action: request.action,
            message: format!("Invalid action. Valid actions are: {:?}", valid_actions),
            request_server_date_time: Utc::now(),
        };
        return Ok(Json(json!(response)));
    }

    // Publish scenario deployment command
    match publisher
        .publish_scenario(&scenario_id, &request.vehicle_id, &request.action)
        .await
    {
        Ok(()) => {
            info!(
                "Scenario {} deployment ({}) sent to vehicle {}",
                scenario_id, request.action, request.vehicle_id
            );
            let response = ScenarioDeployResponse {
                success: true,
                scenario_id,
                vehicle_id: request.vehicle_id,
                action: request.action,
                message: "Deployment command sent successfully".to_string(),
                request_server_date_time: Utc::now(),
            };
            Ok(Json(json!(response)))
        }
        Err(e) => {
            error!("Failed to deploy scenario: {}", e);
            let response = ScenarioDeployResponse {
                success: false,
                scenario_id,
                vehicle_id: request.vehicle_id,
                action: request.action,
                message: format!("Failed to send deployment command: {}", e),
                request_server_date_time: Utc::now(),
            };
            Ok(Json(json!(response)))
        }
    }
}
