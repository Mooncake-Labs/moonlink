use tracing_subscriber::Layer;

pub fn init_logging() {
    use tracing_subscriber::{
        fmt, fmt::time, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
    };

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer()
        .with_timer(time::ChronoLocal::new("%Y-%m-%d %H:%M:%S%:z".to_string()))
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(false)
        .with_filter(env_filter);

    // Compose the subscriber.
    let subscriber = Registry::default().with(fmt_layer);

    #[cfg(feature = "profiling")]
    let subscriber = subscriber.with(console_subscriber::spawn());

    let _ = subscriber.try_init();
}
