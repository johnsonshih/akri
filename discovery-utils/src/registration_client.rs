use crate::discovery::v0::{QueryDeviceInfoRequest, QueryDeviceInfoResponse};

use super::discovery::v0::{
    registration_client::RegistrationClient, Device, RegisterDiscoveryHandlerRequest,
};
use log::{error, info, trace};
use std::collections::HashMap;
use std::convert::TryFrom;
use tonic::{
    transport::{Endpoint, Uri},
    Request,
};

pub async fn register_discovery_handler(
    register_request: &RegisterDiscoveryHandlerRequest,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    info!("register_discovery_handler - entered");
    loop {
        // We will ignore this dummy uri because UDS does not use it.
        // Some servers will check the uri content so the uri needs to
        // be in valid format even it's not used, the scheme part is used
        // to specific what scheme to use, such as http or https
        if let Ok(channel) = Endpoint::try_from("http://[::1]:50051")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                tokio::net::UnixStream::connect(super::get_registration_socket())
            }))
            .await
        {
            let mut client = RegistrationClient::new(channel);
            let request = Request::new(register_request.clone());
            client.register_discovery_handler(request).await?;
            break;
        }
        trace!("register_discovery_handler - sleeping for 10 seconds and trying again");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
    Ok(())
}

/// Continually waits for message to re-register with an Agent
pub async fn register_discovery_handler_again(
    mut register_receiver: tokio::sync::mpsc::Receiver<()>,
    register_request: &RegisterDiscoveryHandlerRequest,
) {
    loop {
        match register_receiver.recv().await {
            Some(_) => {
                info!("register_again - received signal ... registering with Agent again");
                register_discovery_handler(register_request).await.unwrap();
            }
            None => {
                info!("register_again - connection to register_again_sender closed ... error")
            }
        }
    }
}

pub async fn query_device_info(
    query_request: &QueryDeviceInfoRequest,
) -> Result<QueryDeviceInfoResponse, Box<dyn std::error::Error + Send + Sync + 'static>> {
    info!("query_device_info - entered");
    // We will ignore this dummy uri because UDS does not use it.
    // Some servers will check the uri content so the uri needs to
    // be in valid format even it's not used, the scheme part is used
    // to specific what scheme to use, such as http or https
    let channel = Endpoint::try_from("http://[::1]:50051")?
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            tokio::net::UnixStream::connect(super::get_registration_socket())
        }))
        .await?;
    let mut client = RegistrationClient::new(channel);
    let request = Request::new(query_request.clone());
    match client.query_device_info(request).await {
        Ok(r) => Ok(r.into_inner()),
        Err(e) => {
            error!("query_device_info error: {:?}", e);
            Err(e.into())
        }
    }
}

#[derive(Clone)]
pub struct DeviceQueryInput {
    pub id: String,
    pub payload: Option<String>,
    pub device: Device,
}

#[derive(Serialize, Debug, Default, Deserialize)]
struct QueryDeviceResponseBody {
    pub result: String,
    pub properties: HashMap<String, String>,
}

pub async fn query_devices(
    query_device_url: Option<&String>,
    queries: Vec<DeviceQueryInput>,
) -> Vec<Device> {
    if let Some(query_url) = query_device_url {
        let mut fetch_devices_tasks = Vec::new();
        for device_query in queries.clone() {
            fetch_devices_tasks.push(tokio::spawn(query_device(query_url.clone(), device_query)));
        }
        let result = futures::future::try_join_all(fetch_devices_tasks).await;
        if let Ok(query_devices_result) = result {
            return query_devices_result
                .into_iter()
                .flatten()
                .collect::<Vec<Device>>();
        }
    }

    // If there is no need to query additional device information or
    // the previous device query is not completely successful, simple
    // assembl Devices and return
    queries
        .into_iter()
        .map(|device_query| device_query.device)
        .collect::<Vec<Device>>()
}

async fn query_device(query_uri: String, query_input: DeviceQueryInput) -> Option<Device> {
    match query_input.payload {
        Some(query_payload) => {
            let request = QueryDeviceInfoRequest {
                payload: query_payload.clone(),
                uri: query_uri,
            };
            let result = query_device_info(&request).await;
            match result {
                Ok(query_device_response) => {
                    info!(
                        "Success to query {query_payload}: {:?}",
                        query_device_response
                    );

                    let query_response = serde_json::from_str::<QueryDeviceResponseBody>(
                        &query_device_response.query_device_result,
                    )
                    .unwrap_or_default();
                    if query_response.result.to_lowercase() != "accept" {
                        None
                    } else {
                        let mut updated_device = query_input.device.clone();
                        updated_device.properties.extend(
                            query_response
                                .properties
                                .iter()
                                .map(|(k, v)| (k.to_uppercase(), v.to_string())),
                        );
                        Some(updated_device)
                    }
                }
                Err(e) => {
                    error!("Fail to query {query_payload}: {e}");
                    // Drop the device if query fails
                    None
                }
            }
        }
        None => Some(query_input.device.clone()),
    }
}
