use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::error::Result;
use arrow_flight_sql_client::arrow_flight_protocol::flight_service_client::FlightServiceClient;
use arrow_flight_sql_client::arrow_flight_protocol::*;
use arrow_flight_sql_client::arrow_flight_protocol_sql::*;
use arrow_flight_sql_client::client::FlightSqlServiceClient;
use arrow_flight_sql_client::client::*;
use arrow_flight_sql_client::tracing::setup_tracing;
use clap::{Args, Parser, Subcommand};
use opentelemetry::global;
use std::cell::RefCell;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::info_span;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Execute(ExecuteArgs),
    ExecuteUpdate(ExecuteUpdateArgs),
    GetCatalogs(GetCatalogsArgs),
    GetTableTypes(GetTableTypesArgs),
    GetSchemas(GetSchemasArgs),
    GetTables(GetTablesArgs),
    GetExportedKeys(GetExportedKeysArgs),
    GetImportedKeys(GetImportedKeysArgs),
    GetPrimaryKeys(GetPrimaryKeysArgs),
}

#[derive(Args, Debug)]
struct Common {
    #[clap(long, default_value_t = String::from("localhost"))]
    hostname: String,
    #[clap(short, long, default_value_t = 52358, parse(try_from_str))]
    port: usize,
    #[clap(long, default_value_t = String::from("http://localhost:4317"))]
    otlp_endpoint: String,
}

#[derive(Args, Debug)]
struct ExecuteArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    query: String,
}

#[derive(Args, Debug)]
struct ExecuteUpdateArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    query: String,
}

#[derive(Args, Debug)]
struct GetCatalogsArgs {
    #[clap(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct GetTableTypesArgs {
    #[clap(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct GetSchemasArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema_filter_pattern: Option<String>,
}

#[derive(Args, Debug)]
struct GetTablesArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema_filter_pattern: Option<String>,
    #[clap(short, long)]
    table_name_filter_pattern: Option<String>,
    #[clap(short, long)]
    include_schema: bool,
}

#[derive(Args, Debug)]
struct GetExportedKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

#[derive(Args, Debug)]
struct GetImportedKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

#[derive(Args, Debug)]
struct GetPrimaryKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

#[tracing::instrument(skip_all)]
async fn new_client(hostname: &String, port: &usize) -> Result<FlightSqlServiceClient<Channel>> {
    let client_address = format!("http://{}:{}", hostname, port);
    let inner = FlightServiceClient::connect(client_address)
        .await
        .map_err(transport_error_to_arrow_erorr)?;
    Ok(FlightSqlServiceClient::new(RefCell::new(inner)))
}

#[tracing::instrument(skip_all)]
async fn get_and_print(mut client: FlightSqlServiceClient<Channel>, fi: FlightInfo) -> Result<()> {
    let first_endpoint = fi.endpoint.first().ok_or(ArrowError::ComputeError(
        "Failed to get first endpoint".to_string(),
    ))?;

    let first_ticket = first_endpoint
        .ticket
        .clone()
        .ok_or(ArrowError::ComputeError(
            "Failed to get first ticket".to_string(),
        ))?;

    let mut flight_data_stream = client.do_get(first_ticket).await?;

    let arrow_schema = arrow_schema_from_flight_info(&fi)?;
    let arrow_schema_ref = SchemaRef::new(arrow_schema);

    print_flight_data_stream(arrow_schema_ref, &mut flight_data_stream).await
}

#[tokio::main]
async fn main() -> Result<()> {
    if let Err(e) = parse_cli().await {
        global::shutdown_tracer_provider();
        panic!("Error in the client: {}", e);
    }

    global::shutdown_tracer_provider();
    Ok(())
}

async fn parse_cli() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Execute(ExecuteArgs {
            common:
                Common {
                    hostname,
                    port,
                    otlp_endpoint,
                },
            query,
        }) => {
            setup_tracing(otlp_endpoint).await;
            let _parent_span = info_span!("execute command").entered();

            let mut client = new_client(hostname, port).await?;
            let fi = client.execute(query.to_string()).await?;
            get_and_print(client, fi).await
        }
        Commands::ExecuteUpdate(ExecuteUpdateArgs {
            common:
                Common {
                    hostname,
                    port,
                    otlp_endpoint,
                },
            query,
        }) => {
            setup_tracing(otlp_endpoint).await;
            let _parent_span = info_span!("execute update command").entered();

            let mut client = new_client(hostname, port).await?;
            let record_count = client.execute_update(query.to_string()).await?;
            println!("Updated {} records.", record_count);
            Ok(())
        }
        Commands::GetCatalogs(GetCatalogsArgs {
            common:
                Common {
                    hostname,
                    port,
                    otlp_endpoint,
                },
        }) => {
            setup_tracing(otlp_endpoint).await;
            let _parent_span = info_span!("get catalogs command").entered();

            let mut client = new_client(hostname, port).await?;
            let fi = client.get_catalogs().await?;
            get_and_print(client, fi).await
        }
        Commands::GetTableTypes(GetTableTypesArgs {
            common:
                Common {
                    hostname,
                    port,
                    otlp_endpoint,
                },
        }) => {
            setup_tracing(otlp_endpoint).await;
            let _parent_span = info_span!("get table types command").entered();

            let mut client = new_client(hostname, port).await?;
            let fi = client.get_table_types().await?;
            get_and_print(client, fi).await
        }
        Commands::GetSchemas(GetSchemasArgs {
            common:
                Common {
                    hostname,
                    port,
                    otlp_endpoint,
                },
            catalog,
            db_schema_filter_pattern: schema,
        }) => {
            setup_tracing(otlp_endpoint).await;
            let _parent_span = info_span!("get schemas command").entered();

            let mut client = new_client(hostname, port).await?;
            let fi = client
                .get_db_schemas(CommandGetDbSchemas {
                    catalog: catalog.as_deref().map(|x| x.to_string()),
                    db_schema_filter_pattern: schema.as_deref().map(|x| x.to_string()),
                })
                .await?;
            get_and_print(client, fi).await
        }
        Commands::GetTables(GetTablesArgs {
            common:
                Common {
                    hostname,
                    port,
                    otlp_endpoint,
                },
            catalog,
            db_schema_filter_pattern,
            table_name_filter_pattern,
            include_schema,
        }) => {
            setup_tracing(otlp_endpoint).await;
            let _parent_span = info_span!("get tables command").entered();

            let mut client = new_client(hostname, port).await?;
            let fi = client
                .get_tables(CommandGetTables {
                    catalog: catalog.as_deref().map(|x| x.to_string()),
                    db_schema_filter_pattern: db_schema_filter_pattern
                        .as_deref()
                        .map(|x| x.to_string()),
                    table_name_filter_pattern: table_name_filter_pattern
                        .as_deref()
                        .map(|x| x.to_string()),
                    table_types: vec![],
                    include_schema: *include_schema,
                })
                .await?;
            get_and_print(client, fi).await
        }
        Commands::GetExportedKeys(GetExportedKeysArgs {
            common:
                Common {
                    hostname,
                    port,
                    otlp_endpoint,
                },
            catalog,
            db_schema,
            table,
        }) => {
            setup_tracing(otlp_endpoint).await;
            let _parent_span = info_span!("get exported keys command").entered();

            let mut client = new_client(hostname, port).await?;
            let fi = client
                .get_exported_keys(CommandGetExportedKeys {
                    catalog: catalog.as_deref().map(|x| x.to_string()),
                    db_schema: db_schema.as_deref().map(|x| x.to_string()),
                    table: table.to_string(),
                })
                .await?;
            get_and_print(client, fi).await
        }
        Commands::GetImportedKeys(GetImportedKeysArgs {
            common:
                Common {
                    hostname,
                    port,
                    otlp_endpoint,
                },
            catalog,
            db_schema,
            table,
        }) => {
            setup_tracing(otlp_endpoint).await;
            let _parent_span = info_span!("get imported keys command").entered();

            let mut client = new_client(hostname, port).await?;
            let fi = client
                .get_imported_keys(CommandGetImportedKeys {
                    catalog: catalog.as_deref().map(|x| x.to_string()),
                    db_schema: db_schema.as_deref().map(|x| x.to_string()),
                    table: table.to_string(),
                })
                .await?;
            get_and_print(client, fi).await
        }
        Commands::GetPrimaryKeys(GetPrimaryKeysArgs {
            common:
                Common {
                    hostname,
                    port,
                    otlp_endpoint,
                },
            catalog,
            db_schema,
            table,
        }) => {
            setup_tracing(otlp_endpoint).await;
            let _parent_span = info_span!("get primary keys command").entered();

            let mut client = new_client(hostname, port).await?;
            let fi = client
                .get_primary_keys(CommandGetPrimaryKeys {
                    catalog: catalog.as_deref().map(|x| x.to_string()),
                    db_schema: db_schema.as_deref().map(|x| x.to_string()),
                    table: table.to_string(),
                })
                .await?;
            get_and_print(client, fi).await
        }
    }?;

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn print_flight_data_stream(
    arrow_schema_ref: SchemaRef,
    flight_data_stream: &mut Streaming<FlightData>,
) -> Result<()> {
    while let Some(flight_data) = flight_data_stream
        .message()
        .await
        .map_err(status_to_arrow_error)?
    {
        let arrow_data = arrow_data_from_flight_data(flight_data, &arrow_schema_ref)?;

        if let ArrowFlightData::RecordBatch(record_batch) = arrow_data {
            arrow::util::pretty::print_batches(&[record_batch])?;
        }
    }

    Ok(())
}
