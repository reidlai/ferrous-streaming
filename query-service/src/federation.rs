use std::fmt::Debug;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, StringBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use tracing::{debug, info};

use crate::error::{QueryError, Result};

/// Maps SQLite type names to Arrow DataTypes.
fn sqlite_type_to_arrow(type_str: &str) -> DataType {
    let t = type_str.to_uppercase();
    if t.contains("INT") {
        DataType::Int64
    } else if t.contains("REAL")
        || t.contains("FLOA")
        || t.contains("DOUB")
        || t.contains("NUM")
        || t.contains("DECIMAL")
    {
        DataType::Float64
    } else if t.contains("BOOL") {
        DataType::Boolean
    } else if t.contains("BLOB") || t.contains("BINARY") {
        DataType::Binary
    } else {
        DataType::Utf8
    }
}

/// Escapes a SQL identifier by doubling quotation marks.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Converts a DataFusion `Expr` into a SQLite-compatible SQL fragment.
pub fn expr_to_sqlite_sql(expr: &Expr, schema: &Schema) -> Option<String> {
    match expr {
        Expr::Column(Column { name, .. }) => {
            if schema.field_with_name(name).is_ok() {
                Some(quote_identifier(name))
            } else {
                None
            }
        }
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Boolean(Some(b)) => {
                Some(if *b { "1".to_string() } else { "0".to_string() })
            }
            ScalarValue::Float32(Some(v)) => Some(v.to_string()),
            ScalarValue::Float64(Some(v)) => Some(v.to_string()),
            ScalarValue::Int8(Some(v)) => Some(v.to_string()),
            ScalarValue::Int16(Some(v)) => Some(v.to_string()),
            ScalarValue::Int32(Some(v)) => Some(v.to_string()),
            ScalarValue::Int64(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => Some(format!("'{}'", s.replace('\'', "''"))),
            ScalarValue::Null
            | ScalarValue::Boolean(None)
            | ScalarValue::Float32(None)
            | ScalarValue::Float64(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Utf8View(None) => Some("NULL".to_string()),
            _ => None,
        },
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left_sql = expr_to_sqlite_sql(left, schema)?;
            let right_sql = expr_to_sqlite_sql(right, schema)?;
            let op_sql = match op {
                Operator::Eq => "=",
                Operator::NotEq => "<>",
                Operator::Lt => "<",
                Operator::LtEq => "<=",
                Operator::Gt => ">",
                Operator::GtEq => ">=",
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::LikeMatch => "LIKE",
                Operator::ILikeMatch => "LIKE",
                Operator::NotLikeMatch => "NOT LIKE",
                _ => return None,
            };
            Some(format!("({left_sql} {op_sql} {right_sql})"))
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let expr_sql = expr_to_sqlite_sql(expr, schema)?;
            if list.is_empty() {
                return Some(if *negated {
                    "(1 = 1)".to_string()
                } else {
                    "(1 = 0)".to_string()
                });
            }
            let mut list_sqls = Vec::with_capacity(list.len());
            for item in list {
                list_sqls.push(expr_to_sqlite_sql(item, schema)?);
            }
            let op = if *negated { "NOT IN" } else { "IN" };
            Some(format!("({expr_sql} {op} ({}))", list_sqls.join(", ")))
        }
        Expr::IsNull(inner) => {
            let inner_sql = expr_to_sqlite_sql(inner, schema)?;
            Some(format!("({inner_sql} IS NULL)"))
        }
        Expr::IsNotNull(inner) => {
            let inner_sql = expr_to_sqlite_sql(inner, schema)?;
            Some(format!("({inner_sql} IS NOT NULL)"))
        }
        Expr::Not(inner) => {
            let inner_sql = expr_to_sqlite_sql(inner, schema)?;
            Some(format!("(NOT {inner_sql})"))
        }
        Expr::Cast(Cast { expr, .. }) => expr_to_sqlite_sql(expr, schema),
        _ => None,
    }
}

/// Extensible DataFusion TableProvider for querying operational SQLite databases.
pub struct SqliteTableProvider {
    db_path: String,
    table_name: String,
    schema: SchemaRef,
}

impl Debug for SqliteTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteTableProvider")
            .field("db_path", &self.db_path)
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .finish()
    }
}

impl SqliteTableProvider {
    /// Inspects an operational SQLite database, inferring schema via PRAGMA table_info.
    pub fn try_new(db_path: impl Into<String>, table_name: impl Into<String>) -> Result<Self> {
        let db_path = db_path.into();
        let table_name = table_name.into();

        if db_path != ":memory:" && !db_path.starts_with("file:") && !Path::new(&db_path).exists() {
            return Err(QueryError::Catalog(format!(
                "SQLite database file does not exist: {db_path}"
            )));
        }

        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .map_err(|e| {
            QueryError::Catalog(format!(
                "Failed to open SQLite database at '{db_path}': {e}"
            ))
        })?;

        let escaped_table_name = table_name.replace('"', "\"\"");
        let pragma_sql = format!("PRAGMA table_info(\"{escaped_table_name}\")");
        let mut stmt = conn.prepare(&pragma_sql).map_err(|e| {
            QueryError::Catalog(format!(
                "Failed to query table info for '{table_name}': {e}"
            ))
        })?;

        let mut rows = stmt.query([]).map_err(|e| {
            QueryError::Catalog(format!(
                "Failed to execute PRAGMA table_info for '{table_name}': {e}"
            ))
        })?;

        let mut fields = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| QueryError::Catalog(format!("Failed to read table info row: {e}")))?
        {
            let col_name: String = row
                .get(1)
                .map_err(|e| QueryError::Catalog(format!("Failed to read column name: {e}")))?;
            let col_type: String = row
                .get(2)
                .map_err(|e| QueryError::Catalog(format!("Failed to read column type: {e}")))?;
            let notnull: i64 = row
                .get(3)
                .map_err(|e| QueryError::Catalog(format!("Failed to read column nullable: {e}")))?;

            let data_type = sqlite_type_to_arrow(&col_type);
            let nullable = notnull == 0;
            fields.push(Field::new(col_name, data_type, nullable));
        }

        if fields.is_empty() {
            return Err(QueryError::Catalog(format!(
                "SQLite table '{table_name}' does not exist or has no columns in '{db_path}'"
            )));
        }

        let schema = Arc::new(Schema::new(fields));
        info!(
            "Inferred schema for SQLite table '{table_name}' with {} columns",
            schema.fields().len()
        );

        Ok(Self {
            db_path,
            table_name,
            schema,
        })
    }

    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[async_trait]
impl TableProvider for SqliteTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // 1. Columnar Projection Pushdown
        let (projected_schema, select_sql) = match projection {
            Some(indices) => {
                let mut projected_fields = Vec::new();
                let mut selected_cols = Vec::new();
                for &idx in indices {
                    if let Some(field) = self.schema.fields().get(idx) {
                        projected_fields.push(field.clone());
                        selected_cols.push(quote_identifier(field.name()));
                    }
                }
                let proj_schema = Arc::new(Schema::new(projected_fields));
                let cols_sql = if selected_cols.is_empty() {
                    "1".to_string()
                } else {
                    selected_cols.join(", ")
                };
                (proj_schema, cols_sql)
            }
            None => {
                let selected_cols: Vec<String> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| quote_identifier(f.name()))
                    .collect();
                (self.schema.clone(), selected_cols.join(", "))
            }
        };

        // 2. Filter Pushdown
        let mut pushed_filters = Vec::new();
        for filter in filters {
            if let Some(sql_frag) = expr_to_sqlite_sql(filter, &self.schema) {
                pushed_filters.push(sql_frag);
            }
        }

        let mut sql = format!(
            "SELECT {} FROM {}",
            select_sql,
            quote_identifier(&self.table_name)
        );
        if !pushed_filters.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&pushed_filters.join(" AND "));
        }
        if let Some(lim) = limit {
            if pushed_filters.len() == filters.len() {
                sql.push_str(&format!(" LIMIT {lim}"));
            }
        }

        debug!("Constructed federated SQLite query: {sql}");

        let plan = SqliteScanExec::new(
            self.db_path.clone(),
            self.table_name.clone(),
            sql,
            projected_schema,
        );

        Ok(Arc::new(plan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if expr_to_sqlite_sql(f, &self.schema).is_some() {
                    let has_inexact = f
                        .exists(|node| {
                            Ok(match node {
                                Expr::BinaryExpr(BinaryExpr { op, .. }) => matches!(
                                    op,
                                    Operator::LikeMatch
                                        | Operator::ILikeMatch
                                        | Operator::NotLikeMatch
                                ),
                                Expr::Cast(_) => true,
                                _ => false,
                            })
                        })
                        .unwrap_or(false);

                    if has_inexact {
                        TableProviderFilterPushDown::Inexact
                    } else {
                        TableProviderFilterPushDown::Exact
                    }
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

/// ExecutionPlan implementation that executes SQLite queries non-blockingly via Tokio spawn_blocking.
#[derive(Debug)]
pub struct SqliteScanExec {
    db_path: String,
    table_name: String,
    sql: String,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SqliteScanExec {
    pub fn new(db_path: String, table_name: String, sql: String, schema: SchemaRef) -> Self {
        let eq_props = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Incremental;
        let boundedness = Boundedness::Bounded;

        let properties = Arc::new(PlanProperties::new(
            eq_props,
            partitioning,
            emission_type,
            boundedness,
        ));

        Self {
            db_path,
            table_name,
            sql,
            schema,
            properties,
        }
    }

    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

impl DisplayAs for SqliteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqliteScan: table={}, sql={}", self.table_name, self.sql)
    }
}

impl ExecutionPlan for SqliteScanExec {
    fn name(&self) -> &str {
        "SqliteScan"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Plan(
                "SqliteScanExec does not support children".into(),
            ))
        }
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::common::Result<TreeNodeRecursion>,
    ) -> datafusion::common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "Invalid partition {partition} for SqliteScanExec with 1 partition"
            )));
        }
        let db_path = self.db_path.clone();
        let sql = self.sql.clone();
        let schema = self.schema.clone();
        let batch_size = context.session_config().batch_size();

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::task::spawn_blocking(move || {
            if let Err(e) = run_sqlite_query(&db_path, &sql, schema.clone(), batch_size, tx.clone())
            {
                let _ = tx.blocking_send(Err(datafusion::error::DataFusionError::Execution(
                    e.to_string(),
                )));
            }
        });

        let stream = ReceiverStream { rx };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

struct ReceiverStream {
    rx: tokio::sync::mpsc::Receiver<datafusion::common::Result<RecordBatch>>,
}

impl futures::Stream for ReceiverStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

enum ColumnBuilder {
    Int64(Int64Builder),
    Int32(Int32Builder),
    Int16(Int16Builder),
    Int8(Int8Builder),
    UInt64(UInt64Builder),
    UInt32(UInt32Builder),
    UInt16(UInt16Builder),
    UInt8(UInt8Builder),
    Float64(Float64Builder),
    Float32(Float32Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
}

impl ColumnBuilder {
    fn new_for_type(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Int32 => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int16 => ColumnBuilder::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int8 => ColumnBuilder::Int8(Int8Builder::with_capacity(capacity)),
            DataType::UInt64 => ColumnBuilder::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::UInt32 => ColumnBuilder::UInt32(UInt32Builder::with_capacity(capacity)),
            DataType::UInt16 => ColumnBuilder::UInt16(UInt16Builder::with_capacity(capacity)),
            DataType::UInt8 => ColumnBuilder::UInt8(UInt8Builder::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Float32 => ColumnBuilder::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Binary => {
                ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 32))
            }
            _ => ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
        }
    }

    fn append_sqlite_value(&mut self, val: rusqlite::types::ValueRef<'_>) {
        use rusqlite::types::ValueRef;
        match self {
            ColumnBuilder::Int64(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(v) => b.append_value(v),
                ValueRef::Real(v) => b.append_value(v as i64),
                ValueRef::Text(v) => {
                    let s = std::str::from_utf8(v).unwrap_or("0");
                    b.append_value(s.parse::<i64>().unwrap_or(0));
                }
                _ => b.append_null(),
            },
            ColumnBuilder::Int32(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(v) => b.append_value(v as i32),
                _ => b.append_null(),
            },
            ColumnBuilder::Int16(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(v) => b.append_value(v as i16),
                _ => b.append_null(),
            },
            ColumnBuilder::Int8(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(v) => b.append_value(v as i8),
                _ => b.append_null(),
            },
            ColumnBuilder::UInt64(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(v) => b.append_value(v as u64),
                _ => b.append_null(),
            },
            ColumnBuilder::UInt32(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(v) => b.append_value(v as u32),
                _ => b.append_null(),
            },
            ColumnBuilder::UInt16(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(v) => b.append_value(v as u16),
                _ => b.append_null(),
            },
            ColumnBuilder::UInt8(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(v) => b.append_value(v as u8),
                _ => b.append_null(),
            },
            ColumnBuilder::Float64(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Real(v) => b.append_value(v),
                ValueRef::Integer(v) => b.append_value(v as f64),
                _ => b.append_null(),
            },
            ColumnBuilder::Float32(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Real(v) => b.append_value(v as f32),
                ValueRef::Integer(v) => b.append_value(v as f32),
                _ => b.append_null(),
            },
            ColumnBuilder::Boolean(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(v) => b.append_value(v != 0),
                ValueRef::Text(v) => {
                    let s = std::str::from_utf8(v).unwrap_or("false");
                    b.append_value(s == "1" || s.eq_ignore_ascii_case("true"));
                }
                _ => b.append_null(),
            },
            ColumnBuilder::Utf8(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Text(v) => b.append_value(std::str::from_utf8(v).unwrap_or_default()),
                ValueRef::Integer(v) => b.append_value(v.to_string()),
                ValueRef::Real(v) => b.append_value(v.to_string()),
                _ => b.append_null(),
            },
            ColumnBuilder::Binary(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Blob(v) => b.append_value(v),
                _ => b.append_null(),
            },
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(b) => Arc::new(b.finish()),
            ColumnBuilder::Int16(b) => Arc::new(b.finish()),
            ColumnBuilder::Int8(b) => Arc::new(b.finish()),
            ColumnBuilder::UInt64(b) => Arc::new(b.finish()),
            ColumnBuilder::UInt32(b) => Arc::new(b.finish()),
            ColumnBuilder::UInt16(b) => Arc::new(b.finish()),
            ColumnBuilder::UInt8(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(b) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(b) => Arc::new(b.finish()),
            ColumnBuilder::Binary(b) => Arc::new(b.finish()),
        }
    }
}

fn create_builders(schema: &SchemaRef, batch_size: usize) -> Vec<ColumnBuilder> {
    schema
        .fields()
        .iter()
        .map(|f| ColumnBuilder::new_for_type(f.data_type(), batch_size))
        .collect()
}

fn run_sqlite_query(
    db_path: &str,
    sql: &str,
    schema: SchemaRef,
    batch_size: usize,
    tx: tokio::sync::mpsc::Sender<datafusion::common::Result<RecordBatch>>,
) -> Result<()> {
    let conn = rusqlite::Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|e| QueryError::Execution(format!("Failed to open SQLite database: {e}")))?;
    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .ok();

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| QueryError::Execution(format!("Failed to prepare SQL query '{sql}': {e}")))?;

    let num_fields = schema.fields().len();
    let mut builders = create_builders(&schema, batch_size);
    let mut row_count = 0;
    let mut total_rows = 0;

    let mut rows = stmt
        .query([])
        .map_err(|e| QueryError::Execution(format!("Failed to execute SQL query '{sql}': {e}")))?;

    while let Some(row) = rows
        .next()
        .map_err(|e| QueryError::Execution(format!("Failed to fetch SQLite row: {e}")))?
    {
        for (i, builder) in builders.iter_mut().enumerate() {
            let val_ref = row.get_ref(i).unwrap_or(rusqlite::types::ValueRef::Null);
            builder.append_sqlite_value(val_ref);
        }
        row_count += 1;
        total_rows += 1;

        if row_count >= batch_size {
            let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
            let batch = if num_fields == 0 {
                RecordBatch::try_new_with_options(
                    schema.clone(),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(row_count)),
                )
                .map_err(|e| QueryError::Execution(e.to_string()))?
            } else {
                RecordBatch::try_new(schema.clone(), arrays)
                    .map_err(|e| QueryError::Execution(e.to_string()))?
            };

            if tx.blocking_send(Ok(batch)).is_err() {
                return Ok(());
            }

            builders = create_builders(&schema, batch_size);
            row_count = 0;
        }
    }

    if row_count > 0 {
        let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
        let batch = if num_fields == 0 {
            RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(row_count)),
            )
            .map_err(|e| QueryError::Execution(e.to_string()))?
        } else {
            RecordBatch::try_new(schema.clone(), arrays)
                .map_err(|e| QueryError::Execution(e.to_string()))?
        };
        let _ = tx.blocking_send(Ok(batch));
    } else if total_rows == 0 {
        let batch = RecordBatch::new_empty(schema);
        let _ = tx.blocking_send(Ok(batch));
    }

    Ok(())
}
