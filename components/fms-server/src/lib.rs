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

use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};

use axum::extract::{Query, State};
use influx_client::connection::InfluxConnectionConfig;
use log::{error, info, warn};

use serde_json::json;
use std::collections::HashMap;
use std::process;
use std::sync::Arc;

use chrono::Utc;

use influx_reader::InfluxReader;

mod influx_reader;
mod models;
mod pullpiri;
mod pullpiri_publisher;
mod query_parser;

use pullpiri::AppState;
use pullpiri_publisher::ScenarioPublisher;

use models::position::{
    VehiclePositionResponseObject, VehiclePositionResponseObjectVehiclePositionResponse,
};
use models::status::{
    VehicleStatusResponseObject, VehicleStatusResponseObjectVehicleStatusResponse,
};
use query_parser::parse_query_parameters;

pub async fn app(influx_connection_params: &InfluxConnectionConfig) -> Router {
    let influx_reader = InfluxReader::new(influx_connection_params).map_or_else(
        |e| {
            error!("failed to create InfluxDB client: {e}");
            process::exit(1);
        },
        Arc::new,
    );

    // Initialize scenario publisher (optional - enabled when ZENOH_CONFIG is set)
    let scenario_publisher = if let Some(config) = pullpiri_publisher::PublisherConfig::from_env() {
        match ScenarioPublisher::new(&config).await {
            Ok(publisher) => {
                info!("Scenario publisher initialized");
                Some(Arc::new(publisher))
            }
            Err(e) => {
                warn!("Failed to create scenario publisher: {}", e);
                None
            }
        }
    } else {
        info!("Scenario publisher disabled (ZENOH_CONFIG not set)");
        None
    };

    let app_state = Arc::new(AppState {
        influx_reader,
        scenario_publisher,
    });

    info!("starting rFMS server");
    Router::new()
        // Existing rFMS API routes
        .route("/", get(root))
        .route("/rfms/vehiclepositions", get(get_vehicleposition))
        .route("/rfms/vehicles", get(get_vehicles))
        .route("/rfms/vehiclestatuses", get(get_vehiclesstatuses))
        // PULLPIRI API routes
        .route("/pullpiri/status", get(pullpiri::get_status))
        .route("/pullpiri/workloads", get(pullpiri::get_workloads))
        .route("/pullpiri/scenarios", get(pullpiri::get_scenarios))
        .route("/pullpiri/scenarios/:id/deploy", post(pullpiri::deploy_scenario))
        .with_state(app_state)
}

async fn root() -> &'static str {
    "Welcome to the rFMS server. The following endpoints are implemented: '/rfms/vehicleposition', '/rfms/vehicles', and '/rfms/vehiclestatuses'"
}

async fn get_vehicleposition(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let query_parameters = parse_query_parameters(&params)?;

    app_state
        .influx_reader
        .get_vehicleposition(&query_parameters)
        .await
        .map(|positions| {
            let result = json!(VehiclePositionResponseObject {
                vehicle_position_response: VehiclePositionResponseObjectVehiclePositionResponse {
                    vehicle_positions: Some(positions)
                },
                more_data_available: false,
                more_data_available_link: None,
                request_server_date_time: chrono::Utc::now()
            });

            Json(result)
        })
        .map_err(|e| {
            error!("error retrieving vehicle positions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn get_vehicles(
    State(app_state): State<Arc<AppState>>,
    Query(_params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    app_state
        .influx_reader
        .get_vehicles()
        .await
        .map(|vehicles| {
            let response = models::vehicle::VehicleResponseObjectVehicleResponse {
                vehicles: Some(vehicles),
            };

            let result_object = json!(models::vehicle::VehicleResponseObject {
                vehicle_response: response,
                more_data_available: false,
                more_data_available_link: None,
            });
            Json(result_object)
        })
        .map_err(|e| {
            error!("error retrieving vehicles: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn get_vehiclesstatuses(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let query_parameters = parse_query_parameters(&params)?;
    app_state
        .influx_reader
        .get_vehiclesstatuses(&query_parameters)
        .await
        .map(|vehicles_statuses| {
            let response = VehicleStatusResponseObjectVehicleStatusResponse {
                vehicle_statuses: Some(vehicles_statuses),
            };

            //TODO for request_server_date_time
            // put in start time used in influx query instead of now
            let result_object = json!(VehicleStatusResponseObject {
                vehicle_status_response: response,
                more_data_available: false,
                more_data_available_link: None,
                request_server_date_time: Utc::now()
            });
            Json(result_object)
        })
        .map_err(|e| {
            error!("error retrieving vehicle statuses: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}
