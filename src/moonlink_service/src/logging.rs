use tracing_subscriber::{
    fmt, fmt::time, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer, Registry,
};

/// Install a global tracing subscriber that streams logs to stdout. Inside the pg_mooncake
/// background worker this ends up in the Postgres server log.
pub fn init_logging() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer()
        .with_timer(time::ChronoLocal::new("%Y-%m-%d %H:%M:%S%:z".to_string()))
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(false)
        .with_filter(env_filter);

    let _ = Registry::default().with(fmt_layer).try_init();
}
