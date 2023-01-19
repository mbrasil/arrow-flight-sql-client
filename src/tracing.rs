use opentelemetry::{
    global,
    runtime::Tokio,
    sdk::{propagation::TraceContextPropagator, trace, trace::Tracer, Resource},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tracing::{debug, subscriber};
use tracing_subscriber::{filter, layer::SubscriberExt, EnvFilter, Layer, Registry};

///  Create the opentelemetry::sdk::trace::Tracer to use in the telemetry layer.
/// * `otlp_endpoint` - The opentelemetry collector endpoint.
async fn create_opentelemetry_tracer(otlp_endpoint: String) -> Tracer {
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(otlp_endpoint),
        )
        .with_trace_config(
            trace::config().with_resource(Resource::new(vec![KeyValue::new(
                SERVICE_NAME,
                env!("CARGO_PKG_NAME"),
            )])),
        )
        .install_batch(Tokio)
        .unwrap()
}

///  Creates the tracing layers and inits the tracing subscriber.
/// * `otlp_endpoint` - The opentelemetry collector endpoint.
pub async fn setup_tracing(otlp_endpoint: &String) {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("warn,arrow_flight_sql_client=debug"))
        .unwrap();

    let log_layer = tracing_subscriber::fmt::layer();

    let tracer = create_opentelemetry_tracer(otlp_endpoint.to_string()).await;

    let telemetry_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_exception_field_propagation(true)
        .with_tracked_inactivity(true)
        .with_filter(filter::LevelFilter::INFO);

    let collector = Registry::default()
        .with(env_filter)
        .with(log_layer)
        .with(telemetry_layer);

    subscriber::set_global_default(collector).unwrap();

    debug!(
        "Telemetry subscriber initiated for the OpenTelemetry endpoint [{}].",
        otlp_endpoint
    );
}
