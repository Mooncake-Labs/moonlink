use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() {
    let ctx = SessionContext::new();

    let df = ctx
        .sql("SELECT * FROM generate_series(1, 10)")
        .await
        .expect("select failed");
    df.show().await.expect("show failed");
}
