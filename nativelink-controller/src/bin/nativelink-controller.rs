// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, PodSpec, PodTemplateSpec, Volume,
    VolumeMount,
};
use kube::api::{Api, ObjectMeta, Patch, PatchParams, PostParams, ResourceExt};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Client, Resource};
use nativelink_controller::controller::{NativeLink, NativeLinkStatus};
use tracing::*;

#[derive(Clone)]
struct Context {
    client: Client,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    KubeError(#[from] kube::Error),
    #[error(transparent)]
    SerializationError(#[from] serde_json::Error),
}

/// Read the CRD and create or update the ConfigMap containing the json config
/// which we'll mount into pods.
async fn create_or_update_configmap(nativelink: &NativeLink, client: Client) -> Result<(), Error> {
    let namespace = nativelink.namespace().unwrap_or_default();
    let configmaps: Api<ConfigMap> = Api::namespaced(client, &namespace);
    let name = format!("nativelink-config-{}", nativelink.name_any());

    let config_data = serde_json::to_string_pretty(&nativelink.spec.config)?;
    let data = std::collections::BTreeMap::from([("nativelink.json".to_string(), config_data)]);

    let configmap = ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace.clone()),
            owner_references: Some(vec![nativelink.controller_owner_ref(&()).unwrap()]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    };

    match configmaps.get(&name).await {
        Ok(_) => {
            configmaps
                .replace(&name, &PostParams::default(), &configmap)
                .await?;
        }
        Err(kube::Error::Api(api_err)) if api_err.code == 404 => {
            configmaps
                .create(&PostParams::default(), &configmap)
                .await?;
        }
        Err(e) => return Err(Error::KubeError(e)),
    }

    Ok(())
}

/// Read the CRD and create or update the Deployment.
async fn create_or_update_deployment(nativelink: &NativeLink, client: Client) -> Result<(), Error> {
    let namespace = nativelink.namespace().unwrap_or_default();
    let deployments: Api<Deployment> = Api::namespaced(client, &namespace);
    let name = format!("nativelink-deployment-{}", nativelink.name_any());
    let labels = std::collections::BTreeMap::from([("app".to_string(), name.clone())]);

    let configmap_name = format!("nativelink-config-{}", nativelink.name_any());

    let ports = nativelink
        .spec
        .config
        .servers
        .iter()
        .filter_map(|s| {
            if let nativelink_config::cas_server::ListenerConfig::http(listener) = &s.listener {
                listener
                    .socket_address
                    .split(':')
                    .last()
                    .and_then(|p| p.parse().ok())
                    .map(|port| ContainerPort {
                        container_port: port,
                        protocol: Some("TCP".to_string()),
                        ..Default::default()
                    })
            } else {
                None
            }
        })
        .collect();

    let config_file_path = "/etc/nativelink/nativelink.json".to_string();

    let mut args = vec![config_file_path];
    args.extend(nativelink.spec.runtime.args.clone());

    let container = Container {
        name: "nativelink".to_string(),
        image: Some(nativelink.spec.image.clone()),
        args: Some(args),
        env: Some(
            nativelink
                .spec
                .runtime
                .env
                .iter()
                .map(|(k, v)| k8s_openapi::api::core::v1::EnvVar {
                    name: k.clone(),
                    value: Some(v.clone()),
                    ..Default::default()
                })
                .collect(),
        ),
        volume_mounts: Some(vec![VolumeMount {
            name: "nativelink-config".to_string(),
            mount_path: "/etc/nativelink/".to_string(),
            ..Default::default()
        }]),
        working_dir: nativelink.spec.runtime.working_dir.clone(),
        ports: Some(ports),
        ..Default::default()
    };

    let pod_spec = PodSpec {
        containers: vec![container],
        volumes: Some(vec![Volume {
            name: "nativelink-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: configmap_name.clone(),
                ..Default::default()
            }),
            ..Default::default()
        }]),
        ..Default::default()
    };

    let deployment_spec = DeploymentSpec {
        replicas: Some(nativelink.spec.replicas),
        selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
            match_labels: Some(labels.clone()),
            ..Default::default()
        },
        template: PodTemplateSpec {
            metadata: Some(ObjectMeta {
                labels: Some(labels.clone()),
                ..Default::default()
            }),
            spec: Some(pod_spec),
        },
        ..Default::default()
    };

    let deployment = Deployment {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace.clone()),
            owner_references: Some(vec![nativelink.controller_owner_ref(&()).unwrap()]),
            ..Default::default()
        },
        spec: Some(deployment_spec),
        ..Default::default()
    };

    match deployments.get(&name).await {
        Ok(_) => {
            deployments
                .replace(&name, &PostParams::default(), &deployment)
                .await?;
        }
        Err(kube::Error::Api(api_err)) if api_err.code == 404 => {
            deployments
                .create(&PostParams::default(), &deployment)
                .await?;
        }
        Err(e) => return Err(Error::KubeError(e)),
    }

    Ok(())
}

/// Refresh the deployment status.
async fn update_status(nativelink: &NativeLink, client: Client) -> Result<(), Error> {
    let namespace = nativelink.namespace().unwrap_or_default();
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), &namespace);
    let nativelinks: Api<NativeLink> = Api::namespaced(client, &namespace);
    let name = format!("nativelink-deployment-{}", nativelink.name_any());

    let deployment = deployments.get(&name).await?;

    let available_replicas = deployment
        .status
        .as_ref()
        .and_then(|s| s.available_replicas)
        .unwrap_or(0);

    let status = NativeLinkStatus {
        running: available_replicas > 0,
        last_started: Some(Utc::now().to_rfc3339()),
        error: None,
        active_ports: HashMap::new(),
    };

    let status_patch = serde_json::json!({
        "status": status
    });

    nativelinks
        .patch_status(
            &nativelink.name_any(),
            &PatchParams::default(),
            &Patch::Merge(&status_patch),
        )
        .await?;

    Ok(())
}

/// Run a reconciliation loop.
/// TODO(aaronmondal): Consider a non-blocking implementation.
async fn reconcile(nativelink: Arc<NativeLink>, ctx: Arc<Context>) -> Result<Action, Error> {
    let client = ctx.client.clone();

    create_or_update_configmap(&nativelink, client.clone()).await?;

    create_or_update_deployment(&nativelink, client.clone()).await?;

    update_status(&nativelink, client.clone()).await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Error policy to requeue on error.
fn requeue_error_policy(_object: Arc<NativeLink>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!("Reconciliation error: {:?}", error);
    Action::requeue(Duration::from_secs(10))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let client = Client::try_default().await?;

    let nativelinks: Api<NativeLink> = Api::all(client.clone());
    if let Ok(list) = nativelinks.list(&Default::default()).await {
        for item in list {
            info!("Raw item: {}", serde_json::to_string_pretty(&item)?);
        }
    }
    let context = Context {
        client: client.clone(),
    };

    info!("Starting NativeLink controller");
    info!("ASDF");
    Controller::new(nativelinks, watcher::Config::default())
        .shutdown_on_signal()
        .run(reconcile, requeue_error_policy, Arc::new(context))
        .for_each(|res| async move {
            match res {
                Ok(_) => debug!("Reconciliation successful"),
                Err(err) => warn!("Reconciliation error: {:?}", err),
            }
        })
        .await;

    Ok(())
}
