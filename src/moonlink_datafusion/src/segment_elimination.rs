use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::ScalarValue;
use datafusion::common::{Column, Result};
use datafusion::physical_expr::PhysicalExpr;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::schema::types::SchemaDescriptor;
use std::sync::Arc;

/// Represents statistics for a column in a row group
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub data_type: DataType,
}

/// Represents statistics for a row group
#[derive(Debug)]
pub struct RowGroupStatistics {
    pub row_count: i64,
    pub column_stats: Vec<Option<ColumnStatistics>>,
}

/// Represents statistics for a parquet file
#[derive(Debug)]
pub struct FileStatistics {
    pub row_groups: Vec<RowGroupStatistics>,
    pub total_rows: i64,
}

impl FileStatistics {
    /// Create file statistics from parquet metadata
    pub fn from_parquet_metadata(
        metadata: &ParquetMetaData,
        arrow_schema: &Schema,
    ) -> Result<Self> {
        let row_groups_metadata = metadata.row_groups();
        let parquet_schema = metadata.file_metadata().schema_descr();
        let total_rows = metadata.file_metadata().num_rows();

        let mut row_groups = Vec::with_capacity(row_groups_metadata.len());

        for row_group_meta in row_groups_metadata {
            let mut column_stats = Vec::with_capacity(arrow_schema.fields().len());

            for field in arrow_schema.fields().iter() {
                let column_stat =
                    extract_column_statistics(field, row_group_meta, parquet_schema, arrow_schema)?;
                column_stats.push(column_stat);
            }

            row_groups.push(RowGroupStatistics {
                row_count: row_group_meta.num_rows(),
                column_stats,
            });
        }

        Ok(Self {
            row_groups,
            total_rows,
        })
    }
}

/// Extract column statistics from a specific row group and column
fn extract_column_statistics(
    field: &Field,
    row_group_meta: &RowGroupMetaData,
    parquet_schema: &SchemaDescriptor,
    arrow_schema: &Schema,
) -> Result<Option<ColumnStatistics>> {
    // Use Parquet's StatisticsConverter now that we have the full Arrow schema
    let column_name = field.name();

    // Build converter; if the column isn't in Arrow or any error occurs, treat as no stats
    let converter = match StatisticsConverter::try_new(column_name, arrow_schema, parquet_schema) {
        Ok(conv) => conv,
        Err(_) => return Ok(None),
    };

    // If column not present in the parquet file (schema evolution), no stats
    let parquet_col_idx = match converter.parquet_column_index() {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Extract min and max arrays for this single row group
    let mins_arr = match converter.row_group_mins(std::iter::once(row_group_meta)) {
        Ok(arr) => arr,
        Err(_) => return Ok(None),
    };
    let maxs_arr = match converter.row_group_maxes(std::iter::once(row_group_meta)) {
        Ok(arr) => arr,
        Err(_) => return Ok(None),
    };

    // Convert to ScalarValue for index 0
    let min_sv = ScalarValue::try_from_array(mins_arr.as_ref(), 0).ok();
    let max_sv = ScalarValue::try_from_array(maxs_arr.as_ref(), 0).ok();

    // Null count via converter (UInt64Array); treat missing as None
    let null_count = match converter.row_group_null_counts(std::iter::once(row_group_meta)) {
        Ok(arr) => arr.into_iter().next().flatten().map(|v| v as i64),
        Err(_) => None,
    };

    // Distinct count not available via converter; get from raw parquet statistics if present
    let distinct_count = row_group_meta
        .column(parquet_col_idx)
        .statistics()
        .and_then(|s| s.distinct_count_opt())
        .map(|c| c as i64);

    // If both min and max failed to convert, consider no stats
    if min_sv.is_none() && max_sv.is_none() && null_count.is_none() && distinct_count.is_none() {
        return Ok(None);
    }

    Ok(Some(ColumnStatistics {
        min: min_sv,
        max: max_sv,
        null_count,
        distinct_count,
        data_type: field.data_type().clone(),
    }))
}

/// Analyzes a file and returns which row groups should be scanned
pub fn analyze_file_for_segment_elimination(
    file_stats: &FileStatistics,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Result<Vec<bool>> {
    if predicate.is_none() {
        return Ok(vec![true; file_stats.row_groups.len()]);
    }
    let schema_ref = Arc::new(schema.clone());
    let pruning_predicate = datafusion_pruning::PruningPredicate::try_new(
        Arc::clone(predicate.as_ref().unwrap()),
        Arc::clone(&schema_ref),
    )?;
    let stats_adapter = RowGroupPruningStatisticsAdapter::new(file_stats, schema_ref);
    let results = pruning_predicate.prune(&stats_adapter)?;
    Ok(results)
}

struct RowGroupPruningStatisticsAdapter<'a> {
    file_stats: &'a FileStatistics,
    schema: Arc<Schema>,
}

impl<'a> RowGroupPruningStatisticsAdapter<'a> {
    fn new(file_stats: &'a FileStatistics, schema: Arc<Schema>) -> Self {
        Self { file_stats, schema }
    }
}

impl<'a> PruningStatistics for RowGroupPruningStatisticsAdapter<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        let mut has_value = false;
        let mut values = Vec::with_capacity(self.file_stats.row_groups.len());
        for rg in &self.file_stats.row_groups {
            match rg
                .column_stats
                .get(index)
                .and_then(|c| c.as_ref())
                .and_then(|c| c.min.clone())
            {
                Some(v) => {
                    has_value = true;
                    values.push(v);
                }
                None => values.push(ScalarValue::Null),
            }
        }
        if !has_value {
            return None;
        }
        ScalarValue::iter_to_array(values).ok()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        let mut has_value = false;
        let mut values = Vec::with_capacity(self.file_stats.row_groups.len());
        for rg in &self.file_stats.row_groups {
            match rg
                .column_stats
                .get(index)
                .and_then(|c| c.as_ref())
                .and_then(|c| c.max.clone())
            {
                Some(v) => {
                    has_value = true;
                    values.push(v);
                }
                None => values.push(ScalarValue::Null),
            }
        }
        if !has_value {
            return None;
        }
        ScalarValue::iter_to_array(values).ok()
    }

    fn num_containers(&self) -> usize {
        self.file_stats.row_groups.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let index = self.schema.index_of(column.name()).ok()?;
        let values = self.file_stats.row_groups.iter().map(|rg| {
            rg.column_stats
                .get(index)
                .and_then(|c| c.as_ref())
                .and_then(|c| c.null_count)
                .and_then(|v| u64::try_from(v).ok())
        });
        Some(Arc::new(UInt64Array::from_iter(values)) as ArrayRef)
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let values = self
            .file_stats
            .row_groups
            .iter()
            .map(|rg| u64::try_from(rg.row_count).ok());
        Some(Arc::new(UInt64Array::from_iter(values)) as ArrayRef)
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<arrow::array::BooleanArray> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_analyze_file_for_segment_elimination() {
        let file_stats = FileStatistics {
            row_groups: vec![
                RowGroupStatistics {
                    row_count: 1000,
                    column_stats: vec![Some(ColumnStatistics {
                        min: Some(ScalarValue::Int32(Some(1))),
                        max: Some(ScalarValue::Int32(Some(100))),
                        null_count: Some(0),
                        distinct_count: Some(100),
                        data_type: DataType::Int32,
                    })],
                },
                RowGroupStatistics {
                    row_count: 1000,
                    column_stats: vec![Some(ColumnStatistics {
                        min: Some(ScalarValue::Int32(Some(101))),
                        max: Some(ScalarValue::Int32(Some(200))),
                        null_count: Some(0),
                        distinct_count: Some(100),
                        data_type: DataType::Int32,
                    })],
                },
                RowGroupStatistics {
                    row_count: 1000,
                    column_stats: vec![Some(ColumnStatistics {
                        min: Some(ScalarValue::Int32(Some(201))),
                        max: Some(ScalarValue::Int32(Some(300))),
                        null_count: Some(0),
                        distinct_count: Some(100),
                        data_type: DataType::Int32,
                    })],
                },
            ],
            total_rows: 3000,
        };

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        // Test with no predicate - should include all row groups
        let include_all = analyze_file_for_segment_elimination(&file_stats, None, &schema).unwrap();
        assert_eq!(include_all, vec![true, true, true]);

        // Test id > 150 - should exclude first row group, include others
        // Build a physical predicate: col("id") > 150
        use datafusion::logical_expr::{col, lit};
        let df_schema = datafusion::common::DFSchema::try_from(schema.clone()).unwrap();
        let logical_pred = col("id").gt(lit(150i32));
        let ctx = datafusion::prelude::SessionContext::new();
        let physical_pred = ctx.create_physical_expr(logical_pred, &df_schema).unwrap();
        let result =
            analyze_file_for_segment_elimination(&file_stats, Some(physical_pred), &schema)
                .unwrap();
        assert_eq!(result, vec![false, true, true]);
    }
}
