mod batches;
mod client;
mod config;
mod core;
mod encoding;
mod external_maintenance;
mod inline_size;
mod metrics;
mod replay_epoch;
mod schema;
mod sql;

use std::fmt;

use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::TableName,
};
use serde::{Deserialize, Serialize};

/// The DuckDB catalog alias used in every `lake.<table>` qualified name.
pub(super) const LAKE_CATALOG: &str = "lake";

/// A table reference inside the DuckLake catalog.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct DuckLakeTableName {
    schema: String,
    table: String,
}

impl DuckLakeTableName {
    /// Creates a DuckLake table reference from explicit schema and table names.
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self { schema: schema.into(), table: table.into() }
    }

    /// Creates a DuckLake table reference that mirrors a source Postgres table.
    pub fn from_source(table_name: &TableName) -> Self {
        Self::new(table_name.schema.clone(), table_name.name.clone())
    }

    /// Returns the DuckLake schema name.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the DuckLake table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns a stable ID for logs, metrics, and replay marker rows.
    pub fn id(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Serializes this reference for durable destination metadata.
    pub fn to_metadata_id(&self) -> EtlResult<String> {
        serde_json::to_string(self).map_err(|source| {
            etl_error!(
                ErrorKind::InvalidState,
                "DuckLake destination table metadata serialization failed",
                source: source
            )
        })
    }

    /// Parses a durable destination metadata table reference.
    pub(super) fn from_metadata_id(value: &str) -> EtlResult<Self> {
        serde_json::from_str(value).map_err(|source| {
            etl_error!(
                ErrorKind::InvalidState,
                "DuckLake destination table metadata is invalid",
                format!("destination_table_id={value}"),
                source: source
            )
        })
    }

    /// Returns whether this is an internal ETL helper table.
    pub(super) fn is_internal_helper(&self) -> bool {
        self.table.starts_with("__etl_")
    }
}

impl fmt::Display for DuckLakeTableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.id())
    }
}

/// Attach-level DuckLake data inlining limit for streaming ETL writes.
///
/// This applies to every DuckDB connection in the destination pool so small
/// writes inline into the DuckLake metadata first and can later be
/// materialized to Parquet by an external maintenance job.
pub(super) const ATTACH_DATA_INLINING_ROW_LIMIT: u64 = 1_000_000;

/// Environment variable overriding [`ATTACH_DATA_INLINING_ROW_LIMIT`].
const ATTACH_DATA_INLINING_ROW_LIMIT_ENV_VAR: &str = "ETL_ATTACH_DATA_INLINING_ROW_LIMIT";

/// Parses an attach-level data inlining limit, accepting only integers `>= 1`
/// and falling back to [`ATTACH_DATA_INLINING_ROW_LIMIT`] for missing, empty,
/// or out-of-range input.
///
/// `0` is rejected: it would force every write to Parquet, which a
/// MotherDuck-managed DuckLake (managed storage) cannot accept. Pure so it can
/// be unit-tested without mutating process-global environment state.
fn parse_attach_data_inlining_row_limit(raw: Option<&str>) -> u64 {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(ATTACH_DATA_INLINING_ROW_LIMIT)
}

/// Resolves the attach-level data inlining limit, honoring
/// `ETL_ATTACH_DATA_INLINING_ROW_LIMIT` (read once) and falling back to
/// [`ATTACH_DATA_INLINING_ROW_LIMIT`]. Lowering it (to `>= 1`) forces rows past
/// the limit to be written as Parquet data files instead of inlined into the
/// catalog, which requires a DuckLake whose storage accepts external Parquet
/// writes. Invalid values are ignored with a warning.
pub(super) fn attach_data_inlining_row_limit() -> u64 {
    static LIMIT: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
        let raw = std::env::var(ATTACH_DATA_INLINING_ROW_LIMIT_ENV_VAR).ok();
        let limit = parse_attach_data_inlining_row_limit(raw.as_deref());
        if let Some(raw) = raw.as_deref() {
            let valid = raw.trim().parse::<u64>().ok().is_some_and(|n| n >= 1);
            if !raw.trim().is_empty() && !valid {
                tracing::warn!(
                    value = %raw,
                    "invalid {ATTACH_DATA_INLINING_ROW_LIMIT_ENV_VAR}; using default {limit}"
                );
            }
        }
        limit
    });

    *LIMIT
}

/// Connection-level DuckLake data inlining limit during initial copies.
///
/// COPY uses a dedicated connection pool attached with this limit so its
/// batches become Parquet files without persisting a catalog option that must
/// later be changed for streaming.
pub(super) const COPY_DATA_INLINING_ROW_LIMIT: u64 = 0;

pub use core::{
    DuckLakeDestination, DuckLakeExternalMaintenanceConfig, DuckLakeExternalMaintenancePause,
    DuckLakeMaintenanceMode, table_name_to_ducklake_table_name,
};
#[cfg(feature = "test-utils")]
pub use core::{
    arm_pause_next_streaming_write_for_tests, release_paused_streaming_write_for_tests,
    reset_paused_streaming_write_for_tests,
};

#[cfg(feature = "test-utils")]
pub use batches::{
    arm_fail_after_atomic_batch_commit_once_for_tests,
    arm_fail_after_copy_batch_commit_once_for_tests, ducklake_staging_table_creations_for_tests,
    reset_ducklake_test_hooks,
};
pub use config::S3Config;
pub use etl_maintenance::ducklake::{
    CleanupOldFilesMaintenanceConfig, DuckLakeMaintenanceConfig, DuckLakeMaintenanceOutcome,
    ExpireSnapshotsMaintenanceConfig, InlineFlushMaintenanceConfig,
    MergeAdjacentFilesMaintenanceConfig, RewriteDataFilesMaintenanceConfig, run_maintenance_once,
};
pub use external_maintenance::{
    ExternalMaintenanceOperationHistory, ExternalMaintenanceOperationPolicy,
    ExternalMaintenanceOperationRequest, ExternalMaintenanceOperationRun,
    ExternalMaintenanceOperations, ExternalMaintenancePause, ExternalMaintenancePausePolicy,
    ExternalMaintenanceReplicatorState, ExternalMaintenanceReplicatorStatus,
    ExternalMaintenanceRequestOutcome, ExternalMaintenanceRun, ExternalMaintenanceState,
    ExternalMaintenanceStore, ExternalMaintenanceWatcherConfig, PostgresExternalMaintenanceStore,
    run_external_maintenance_watcher,
};

#[cfg(test)]
mod inlining_tests {
    use super::{ATTACH_DATA_INLINING_ROW_LIMIT, parse_attach_data_inlining_row_limit};

    #[test]
    fn parse_attach_data_inlining_row_limit_rejects_zero_and_invalid() {
        assert_eq!(parse_attach_data_inlining_row_limit(Some("10")), 10);
        assert_eq!(parse_attach_data_inlining_row_limit(Some(" 1000000 ")), 1_000_000);
        for invalid in [None, Some(""), Some("0"), Some("-1"), Some("abc")] {
            assert_eq!(
                parse_attach_data_inlining_row_limit(invalid),
                ATTACH_DATA_INLINING_ROW_LIMIT
            );
        }
    }
}
