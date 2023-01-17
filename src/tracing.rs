use opentelemetry::{runtime::Tokio, sdk::{trace, trace::Tracer, Resource}, KeyValue, global};
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tonic::transport::Channel;
use tracing::{debug, subscriber};
use tracing_subscriber::{filter, fmt::format::FmtSpan, layer::SubscriberExt, Layer, Registry};

///  Create the opentelemetry::sdk::trace::Tracer to use in the telemetry layer.
/// * `otlp_endpoint` - The opentelemetry collector endpoint.
async fn create_opentelemetry_tracer(otlp_endpoint: String) -> Tracer {
    let channel = Channel::from_shared(otlp_endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();

    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_channel(channel),
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

    let telemetry_layer = tracing_opentelemetry::layer()
        .with_tracer(create_opentelemetry_tracer(otlp_endpoint.to_string()).await)
        .with_filter(filter::LevelFilter::INFO);

    let log_layer = tracing_subscriber::fmt::layer()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_filter(filter::LevelFilter::DEBUG);

    let collector = Registry::default().with(telemetry_layer).with(log_layer);

    subscriber::set_global_default(collector).unwrap();

    debug!(
        "Telemetry subscriber initiated for the OpenTelemetry endpoint [{}].",
        otlp_endpoint
    );
}
