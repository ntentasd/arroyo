pub mod sink;

use anyhow::anyhow;
use arrow::datatypes::{DataType, Schema};
use arroyo_operator::connector::{Connection, Connector, LookupConnector, MetadataDef};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc::Sender, oneshot::Receiver};
use tracing::info;
use typify::import_types;

pub struct ScyllaConnector {}

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./scylladb.svg");

import_types!(
    schema = "src/scylla/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

import_types!(schema = "src/scylla/table.json");

pub struct ScyllaClient {}

impl ScyllaClient {
    pub async fn new(config: &ScyllaConfig) -> anyhow::Result<Session> {
        let nodes = config
            .connection
            .split(|c| c == ',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect::<Vec<String>>();

        let session: Session = SessionBuilder::new().known_nodes(nodes).build().await?;

        Ok(session)
    }
}

async fn test_inner(
    c: ScyllaConfig,
    tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
) -> anyhow::Result<String> {
    tx.send(TestSourceMessage::info("Connecting to ScyllaDB"))
        .await
        .unwrap();

    let session = ScyllaClient::new(&c).await?;

    tx.send(TestSourceMessage::info(
        "Connected successfully, checking ScyllaDB version",
    ))
    .await
    .unwrap();

    session
        .query_unpaged("SELECT release_version FROM system.local", &[])
        .await
        .map_err(|e| anyhow!("Received error checking ScyllaDB version: {:?}", e))?;

    Ok("Received ScyllaDB version successfully".to_string())
}

impl Connector for ScyllaConnector {
    type ProfileT = ScyllaConfig;
    type TableT = ScyllaTable;

    fn name(&self) -> &'static str {
        "scylla"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "scylla".to_string(),
            name: "Scylla".to_string(),
            icon: ICON.to_string(),
            description: "Write results to Scylla".to_string(),
            enabled: true,
            source: false,
            sink: true,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn metadata_defs(&self) -> &'static [MetadataDef] {
        &[MetadataDef {
            name: "key",
            data_type: DataType::Utf8,
        }]
    }

    fn table_type(&self, _config: Self::ProfileT, _table: Self::TableT) -> ConnectionType {
        ConnectionType::Sink
    }

    fn get_schema(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        s.cloned()
    }

    fn test_profile(&self, profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        info!("Scylla test_profile called");
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let (itx, _rx) = tokio::sync::mpsc::channel(8);
            let message = match test_inner(profile, itx).await {
                Ok(_) => TestSourceMessage::done("Successfully connected to ScyllaDB"),
                Err(e) => {
                    TestSourceMessage::fail(format!("Failed to connect to ScyllaDB: {:?}", e))
                }
            };

            tx.send(message).unwrap();
        });

        Some(rx)
    }

    fn test(
        &self,
        _: &str,
        c: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let resp = match test_inner(c, tx.clone()).await {
                Ok(c) => TestSourceMessage::done(c),
                Err(e) => TestSourceMessage::fail(e.to_string()),
            };

            tx.send(resp).await.unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        s: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection_config = match profile {
            Some(connection_profile) => {
                serde_json::from_value(connection_profile.config.clone())
                    .map_err(|e| anyhow!("Failed to parse connection config: {:?}", e))?
            }
            None => {
                let raw = options.pull_str("addresses")?;

                let connection = Addresses(raw);
                let username = options.pull_opt_str("username")?.map(VarStr::new);
                let password = options.pull_opt_str("password")?.map(VarStr::new);

                ScyllaConfig {
                    connection,
                    username,
                    password,
                }
            }
        };

        // let typ = options.pull_str("type")?;

        let schema = s
            .ok_or_else(|| anyhow!("No schema defined for ScyllaDB connection"))
            .ok();

        self.from_config(
            None,
            name,
            connection_config,
            ScyllaTable {
                connector_type: TableType::Target {
                    keyspace: options.pull_str("target.keyspace")?,
                    table: options.pull_str("target.table")?,
                },
            },
            schema,
        )
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for ScyllaDB connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for ScyllaDB connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Sink,
            schema,
            &config,
            "ScyllaSink".to_string(),
        ))
    }

    fn make_operator(
        &self,
        _profile: Self::ProfileT,
        _table: Self::TableT,
        _config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        Err(anyhow!("ScyllaConnector::make_operator not implemented"))
    }

    fn make_lookup(
        &self,
        _profile: Self::ProfileT,
        _table: Self::TableT,
        _config: OperatorConfig,
        _schema: Arc<Schema>,
    ) -> anyhow::Result<Box<dyn LookupConnector + Send>> {
        Err(anyhow!("ScyllaConnector::make_lookup not implemented"))
    }
}
