use std::{
    any::Any,
    fmt::{self},
    sync::Arc,
};

use arrow::{
    datatypes::SchemaRef,
    util::display::{ArrayFormatter, FormatOptions},
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Constraints,
    datasource::{TableProvider, TableType},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{dml::InsertOp, Expr},
    physical_plan::{
        insert::{DataSink, DataSinkExec},
        metrics::MetricsSet,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use futures::StreamExt;
use snafu::prelude::*;
use tokio::task::JoinSet;

use crate::{
    sql::db_connection_pool::dbconnection::postgresconn::PostgresConnection,
    util::{constraints, on_conflict::OnConflict, retriable_error::check_and_mark_retriable_error},
};

use crate::postgres::Postgres;

use super::{
    to_datafusion_error, DynPostgresConnection, UnableToDowncastDbConnectionSnafu,
    UnableToInsertArrowBatchSnafu,
};

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone)]
pub struct PostgresTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    postgres: Arc<Postgres>,
    on_conflict: Option<OnConflict>,
}

impl PostgresTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        postgres: Postgres,
        on_conflict: Option<OnConflict>,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            postgres: Arc::new(postgres),
            on_conflict,
        })
    }

    pub fn postgres(&self) -> Arc<Postgres> {
        Arc::clone(&self.postgres)
    }
}

#[async_trait]
impl TableProvider for PostgresTableWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(self.postgres.constraints())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(PostgresDataSink::new(
                Arc::clone(&self.postgres),
                op == InsertOp::Overwrite,
                self.on_conflict.clone(),
            )),
            self.schema(),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct PostgresDataSink {
    postgres: Arc<Postgres>,
    overwrite: bool,
    on_conflict: Option<OnConflict>,
}

#[async_trait]
impl DataSink for PostgresDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut num_rows = 0;

        let conn = self.postgres.pool.connect_direct().await.map(Arc::new)?;
        let mut join_set = JoinSet::new();

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;
            let batch_num_rows = batch.num_rows();

            if batch_num_rows == 0 {
                continue;
            };

            num_rows += batch_num_rows as u64;

            let pg = self.postgres.clone();
            let tx = conn.clone();

            join_set.spawn(async move {
                let sql = batch_to_insert_sql(&batch, pg.table_name());

                tx.conn
                    .execute(&sql, &[])
                    .await
                    .context(UnableToInsertArrowBatchSnafu)
                    .map_err(to_datafusion_error)
            });
        }

        for res in join_set.join_all().await {
            match res {
                Ok(_) => (),
                Err(e) => return Err(e.into()),
            }
        }

        Ok(num_rows)
    }
}

impl PostgresDataSink {
    fn new(postgres: Arc<Postgres>, overwrite: bool, on_conflict: Option<OnConflict>) -> Self {
        Self {
            postgres,
            overwrite,
            on_conflict,
        }
    }
}

fn batch_to_insert_sql(batch: &RecordBatch, table_name: &str) -> String {
    let schema = batch.schema();
    let columns = schema
        .fields()
        .iter()
        .map(|f| f.name().to_owned())
        .collect::<Vec<_>>()
        .join(", ");

    let mut values = Vec::new();
    let options = FormatOptions::new().with_null("NULL");

    for row_idx in 0..batch.num_rows() {
        let row_values: Vec<String> = (0..batch.num_columns())
            .map(|col_idx| {
                let array = batch.column(col_idx);
                let format = ArrayFormatter::try_new(array, &options).unwrap();

                if array.data_type().is_numeric() || array.is_null(row_idx) {
                    format.value(row_idx).to_string()
                } else {
                    format!("'{}'", format.value(row_idx).to_string())
                }
            })
            .collect();
        values.push(format!("({})", row_values.join(", ")));
    }

    format!(
        "INSERT INTO {} ({}) VALUES {}",
        table_name,
        columns,
        values.join(", ")
    )
}

impl std::fmt::Debug for PostgresDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PostgresDataSink")
    }
}

impl DisplayAs for PostgresDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "PostgresDataSink")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_batch_to_insert_sql() {
        // 创建测试数据
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), None, Some("Bob")]);
        let score_array = Float64Array::from(vec![Some(85.5), Some(92.0), None]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(score_array),
            ],
        )
        .unwrap();

        let sql = batch_to_insert_sql(&batch, "students");

        let expected = "\
            INSERT INTO students (id, name, score) VALUES \
            (1, 'Alice', 85.5), \
            (2, NULL, 92.0), \
            (3, 'Bob', NULL)";

        assert_eq!(sql, expected);
    }

    #[test]
    fn test_batch_to_insert_sql_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![] as Vec<i32>)),
                Arc::new(StringArray::from(vec![] as Vec<Option<&str>>)),
            ],
        )
        .unwrap();

        let sql = batch_to_insert_sql(&batch, "test_table");
        let expected = "INSERT INTO test_table (id, name) VALUES ";
        assert_eq!(sql, expected);
    }

    #[test]
    fn test_batch_to_insert_sql_special_chars() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("O'Neil's \"quote\"")])),
            ],
        )
        .unwrap();

        let sql = batch_to_insert_sql(&batch, "quotes");
        let expected = "INSERT INTO quotes (id, text) VALUES (1, 'O''Neil''s \"quote\"')";

        assert_eq!(sql, expected);
    }
}
