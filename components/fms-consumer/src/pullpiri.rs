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
use prost::Message;
use std::sync::Arc;

use up_rust::{UListener, UMessage};

use influx_client::pullpiri::PullpiriStatus;
use influx_client::writer::InfluxWriter;

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
        // Extract payload bytes and decode using prost
        let payload = match &msg.payload {
            Some(bytes) => bytes,
            None => {
                warn!("Received PULLPIRI message without payload");
                return;
            }
        };

        match PullpiriStatus::decode(payload.as_ref()) {
            Ok(pullpiri_status) => {
                debug!(
                    "Received PULLPIRI status from {}: {} workloads, {} scenarios",
                    pullpiri_status.vehicle_id,
                    pullpiri_status.workloads.len(),
                    pullpiri_status.scenarios.len()
                );

                self.influx_writer
                    .write_pullpiri_status(&pullpiri_status)
                    .await;
            }
            Err(e) => {
                warn!("Failed to parse PullpiriStatus: {}", e);
            }
        }
    }
}
