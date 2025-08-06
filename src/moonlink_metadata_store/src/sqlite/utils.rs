/// Util function for metadata storage based on sqlite.
use crate::error::Result;

/// Util function to get database filepath and database uri.
pub(crate) fn get_database_uri_and_filepath(
    location: &str,
) -> (
    String, /*database filepath*/
    String, /*database uri*/
) {
    const PREFIX: &str = "sqlite://";

    if location.starts_with(PREFIX) {
        let filepath = location.trim_start_matches(PREFIX).to_string();
        (filepath.clone(), location.to_string())
    } else {
        let uri = format!("sqlite://{location}");
        (location.to_string(), uri)
    }
}

/// Return whether the requested table exists in the current database.
/// Notice, sqlite doesn't support "schema" concept.
pub async fn table_exists(
    sqlite_conn: &sqlx::SqlitePool,
    _schema_name: &str,
    table_name: &str,
) -> Result<bool> {
    let result: Option<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
            .bind(table_name)
            .fetch_optional(sqlite_conn)
            .await?;

    Ok(result.is_some())
}

/// Create metadata storage table, which fails if the table already exists.
pub async fn create_table(sqlite_conn: &sqlx::SqlitePool, statements: &str) -> Result<()> {
    sqlx::query(statements).execute(sqlite_conn).await?;
    Ok(())
}

/// Create table if not exist.
pub async fn create_table_if_non_existent(
    sqlite_conn: &sqlx::SqlitePool,
    _schema_name: &str,
    table_name: &str,
    statements: &str,
) -> Result<()> {
    if table_exists(sqlite_conn, _schema_name, table_name).await? {
        return Ok(());
    }
    create_table(sqlite_conn, statements).await
}
