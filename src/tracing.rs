use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
    runtime::Tokio,
    sdk::{propagation::TraceContextPropagator, trace, trace::Tracer, Resource},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use std::str::FromStr;
use tonic::{
    metadata::{KeyRef, MetadataKey, MetadataMap},
    Request,
};
use tracing::{debug, subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{filter, layer::SubscriberExt, EnvFilter, Layer, Registry};

///  Create the opentelemetry::sdk::trace::Tracer to use in the telemetry layer.
/// * `otlp_endpoint` - The opentelemetry collector endpoint.
fn create_opentelemetry_tracer(otlp_endpoint: String) -> Tracer {
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

    let tracer = create_opentelemetry_tracer(otlp_endpoint.to_string());

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

pub struct MetadataInjector<'a>(&'a mut MetadataMap);

impl<'a> Injector for MetadataInjector<'a> {
    /// Set a key and value in the MetadataMap.  Does nothing if the key or value are not valid inputs
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = MetadataKey::from_str(key) {
            if let Ok(val) = value.parse() {
                self.0.insert(key, val);
            }
        }
    }
}

pub struct MetadataExtractor<'a>(&'a MetadataMap);

impl<'a> Extractor for MetadataExtractor<'a> {
    /// Get a value for a key from the MetadataMap.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                KeyRef::Ascii(v) => v.as_str(),
                KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

pub fn tracing_parent_span_from_req<T>(request: &Request<T>) {
    let cx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataExtractor(request.metadata()))
    });

    tracing::Span::current().set_parent(cx);
}

pub fn tracing_current_span_to_req<T>(request: &mut Request<T>) {
    let cx = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut MetadataInjector(request.metadata_mut()))
    });
}
