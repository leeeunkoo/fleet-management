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

//! Scenario deployment publisher
//!
//! Publishes scenario deployment commands to vehicles via Zenoh.

use log::{debug, error, info};
use std::env;
use std::path::PathBuf;
use zenoh::config::Config;
use zenoh::Session;

/// Configuration for the scenario publisher
#[derive(Debug, Clone)]
pub struct PublisherConfig {
    /// Path to the Zenoh configuration file
    pub zenoh_config_path: PathBuf,
    /// Key expression for scenario deployment
    pub scenario_deploy_key: String,
}

impl PublisherConfig {
    /// Creates a new PublisherConfig from environment variables
    ///
    /// Required environment variables:
    /// - `ZENOH_CONFIG`: Path to Zenoh configuration file
    ///
    /// Optional environment variables:
    /// - `SCENARIO_DEPLOY_KEY`: Key expression for scenario deployment (default: "fms/scenario/deploy")
    pub fn from_env() -> Option<Self> {
        let zenoh_config_path = env::var("ZENOH_CONFIG").ok().map(PathBuf::from)?;

        let scenario_deploy_key = env::var("SCENARIO_DEPLOY_KEY")
            .unwrap_or_else(|_| "fms/scenario/deploy".to_string());

        Some(Self {
            zenoh_config_path,
            scenario_deploy_key,
        })
    }
}

/// Scenario deployment command
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScenarioDeployCommand {
    /// Scenario ID to deploy
    pub scenario_id: String,
    /// Target vehicle ID (VIN)
    pub vehicle_id: String,
    /// Deployment action (e.g., "deploy", "undeploy", "restart")
    pub action: String,
    /// Timestamp of the command
    pub timestamp: i64,
}

/// Publisher for scenario deployment commands
pub struct ScenarioPublisher {
    session: Session,
    scenario_deploy_key: String,
}

impl ScenarioPublisher {
    /// Creates a new ScenarioPublisher with the given configuration
    pub async fn new(config: &PublisherConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let zenoh_config = Config::from_file(&config.zenoh_config_path)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to load Zenoh config: {}", e),
                ))
            })?;

        let session = zenoh::open(zenoh_config).await.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("Failed to open Zenoh session: {}", e),
            ))
        })?;

        info!("Zenoh session opened successfully");

        Ok(Self {
            session,
            scenario_deploy_key: config.scenario_deploy_key.clone(),
        })
    }

    /// Publishes a scenario deployment command to a specific vehicle
    pub async fn publish_scenario(
        &self,
        scenario_id: &str,
        vehicle_id: &str,
        action: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let command = ScenarioDeployCommand {
            scenario_id: scenario_id.to_string(),
            vehicle_id: vehicle_id.to_string(),
            action: action.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        let payload = serde_json::to_vec(&command).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(e)
        })?;

        // Key expression includes vehicle_id for routing
        let key_expr = format!("{}/{}", self.scenario_deploy_key, vehicle_id);

        debug!(
            "Publishing scenario deployment command to {}: {:?}",
            key_expr, command
        );

        self.session
            .put(&key_expr, payload)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                error!("Failed to publish scenario deployment: {}", e);
                Box::new(std::io::Error::other(format!("Failed to publish: {}", e)))
            })?;

        info!(
            "Scenario {} deployment command sent to vehicle {}",
            scenario_id, vehicle_id
        );

        Ok(())
    }
}
