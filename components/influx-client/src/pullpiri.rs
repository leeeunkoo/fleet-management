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

//! PULLPIRI status types
//!
//! Protobuf message definitions for PULLPIRI status from settingsservice.

/// PULLPIRI status message (same as settingsservice proto)
#[derive(Clone, prost::Message)]
pub struct PullpiriStatus {
    #[prost(string, tag = "1")]
    pub vehicle_id: String,

    #[prost(int64, tag = "2")]
    pub timestamp: i64,

    #[prost(message, repeated, tag = "3")]
    pub workloads: Vec<WorkloadStatus>,

    #[prost(message, repeated, tag = "4")]
    pub scenarios: Vec<ScenarioStatus>,
}

/// Workload status information
#[derive(Clone, prost::Message)]
pub struct WorkloadStatus {
    #[prost(string, tag = "1")]
    pub name: String,

    #[prost(string, tag = "2")]
    pub state: String,

    #[prost(string, optional, tag = "3")]
    pub container_id: Option<String>,

    #[prost(int64, optional, tag = "4")]
    pub started_at: Option<i64>,

    #[prost(int64, optional, tag = "5")]
    pub finished_at: Option<i64>,

    #[prost(string, optional, tag = "6")]
    pub error_message: Option<String>,
}

/// Scenario status information
#[derive(Clone, prost::Message)]
pub struct ScenarioStatus {
    #[prost(string, tag = "1")]
    pub name: String,

    #[prost(string, tag = "2")]
    pub state: String,

    #[prost(int32, tag = "3")]
    pub trigger_count: i32,

    #[prost(int64, optional, tag = "4")]
    pub last_triggered: Option<i64>,

    #[prost(string, optional, tag = "5")]
    pub target_workload: Option<String>,
}
