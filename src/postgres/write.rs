use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
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

use super::{to_datafusion_error, DynPostgresConnection, UnableToDowncastDbConnectionSnafu};

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

        let postgres_conn = self.postgres.pool.connect_direct().await.map(Box::new)?;
        let a: &'static mut PostgresConnection = Box::leak(postgres_conn);

        let tx = a
            .conn
            .transaction()
            .await
            .context(super::UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)
            .map(Arc::new)?;

        if self.overwrite {
            self.postgres
                .delete_all_table_data(&tx)
                .await
                .map_err(to_datafusion_error)?;
        }

        let mut join_set = JoinSet::new();

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;
            let batch_num_rows = batch.num_rows();

            if batch_num_rows == 0 {
                continue;
            };

            num_rows += batch_num_rows as u64;

            // constraints::validate_batch_with_constraints(
            //     &[batch.clone()],
            //     self.postgres.constraints(),
            // )
            // .await
            // .context(super::ConstraintViolationSnafu)
            // .map_err(to_datafusion_error)?;

            let pg = self.postgres.clone();
            let ttx = tx.clone();
            let on_conflict = self.on_conflict.clone();

            join_set.spawn(async move {
                pg.insert_batch(&ttx, batch, on_conflict)
                    .await
                    .map_err(to_datafusion_error)
            });
        }

        for res in join_set.join_all().await {
            match res {
                Ok(_) => (),
                Err(e) => return Err(e.into()),
            }
        }

        match Arc::try_unwrap(tx) {
            Ok(tx) => tx.commit().await.map_err(to_datafusion_error)?,
            Err(_) => {
                panic!("Failed to unwrap transaction")
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
