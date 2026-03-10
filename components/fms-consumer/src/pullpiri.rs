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

//! PULLPIRI status listener
//!
//! Receives PULLPIRI status from settingsservice and stores it in InfluxDB.

use log::{debug, warn};
use serde::Deserialize;
use std::sync::Arc;

use up_rust::{UListener, UMessage};

use influx_client::pullpiri::PullpiriStatus;
use influx_client::writer::InfluxWriter;

/// Scenario status from pullpiri
#[derive(Deserialize, Debug, Clone)]
struct ScenarioStatus {
    name: String,
    state: String,
}

/// Simple JSON structure for pullpiri status
#[derive(Deserialize)]
struct PullpiriJsonStatus {
    vehicle_id: String,
    timestamp: i64,
    #[serde(default)]
    scenarios: Vec<ScenarioStatus>,
    #[serde(default)]
    scenario_count: usize,
}

/// PULLPIRI status listener
///
/// Implements the same pattern as VehicleStatusListener
pub struct PullpiriStatusListener {
    influx_writer: Arc<InfluxWriter>,
}

impl PullpiriStatusListener {
    /// Creates a new PullpiriStatusListener
    pub fn new(influx_writer: Arc<InfluxWriter>) -> Self {
        Self { influx_writer }
    }
}

#[async_trait::async_trait]
impl UListener for PullpiriStatusListener {
    async fn on_receive(&self, msg: UMessage) {
        // Extract payload bytes and decode as JSON
        let payload = match &msg.payload {
            Some(bytes) => bytes,
            None => {
                warn!("Received PULLPIRI message without payload");
                return;
            }
        };

        // Parse JSON payload
        match serde_json::from_slice::<PullpiriJsonStatus>(payload.as_ref()) {
            Ok(json_status) => {
                debug!(
                    "Received PULLPIRI status from {}: timestamp={}, scenarios={}",
                    json_status.vehicle_id, json_status.timestamp, json_status.scenario_count
                );

                // Convert scenarios to the format expected by InfluxDB
                let scenarios: Vec<influx_client::pullpiri::ScenarioStatus> = json_status
                    .scenarios
                    .iter()
                    .map(|s| influx_client::pullpiri::ScenarioStatus {
                        name: s.name.clone(),
                        state: s.state.clone(),
                        trigger_count: 0,
                        last_triggered: None,
                        target_workload: None,
                    })
                    .collect();

                // Convert to PullpiriStatus for InfluxDB
                let pullpiri_status = PullpiriStatus {
                    vehicle_id: json_status.vehicle_id,
                    timestamp: json_status.timestamp,
                    workloads: vec![],
                    scenarios,
                };

                self.influx_writer
                    .write_pullpiri_status(&pullpiri_status)
                    .await;
            }
            Err(e) => {
                warn!("Failed to parse PULLPIRI JSON: {}", e);
            }
        }
    }
}
