use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use deltalake::DeltaTable;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

use crate::config::QueryConfig;
use crate::error::{QueryError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnSummary {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSummary {
    pub name: String,
    pub path: String,
    pub columns: Vec<ColumnSummary>,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogSummary {
    pub tables: Vec<TableSummary>,
    pub registered_count: usize,
    pub missing_count: usize,
}

pub struct DynamicDeltaTableProvider {
    table_name: String,
    path: String,
    log_dir: PathBuf,
    materialized: Arc<std::sync::atomic::AtomicBool>,
    table: Arc<tokio::sync::RwLock<DeltaTable>>,
    inner_provider: Arc<std::sync::RwLock<Arc<dyn TableProvider>>>,
}

impl Debug for DynamicDeltaTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicDeltaTableProvider")
            .field("table_name", &self.table_name)
            .field("path", &self.path)
            .finish()
    }
}

async fn materialize_delta_provider(
    provider: Arc<dyn TableProvider>,
    session: &dyn Session,
) -> datafusion::common::Result<Arc<dyn TableProvider>> {
    let schema = provider.schema();
    let plan = provider.scan(session, None, &[], None).await?;
    let task_ctx = session.task_ctx();
    let batches = datafusion::physical_plan::collect(plan, task_ctx).await?;

    let target_partitions = session.config().target_partitions().max(1);
    let partition_count = if batches.is_empty() {
        1
    } else {
        target_partitions.min(batches.len())
    };
    let mut partitions: Vec<Vec<RecordBatch>> =
        (0..partition_count).map(|_| Vec::new()).collect();
    if batches.is_empty() {
        partitions = vec![vec![]];
    } else {
        for (i, batch) in batches.into_iter().enumerate() {
            partitions[i % partition_count].push(batch);
        }
    }

    let mem_table = MemTable::try_new(schema, partitions)?;
    Ok(Arc::new(mem_table))
}

impl DynamicDeltaTableProvider {
    pub async fn try_new(
        table_name: impl Into<String>,
        path: impl Into<String>,
        table: DeltaTable,
        session: &dyn Session,
    ) -> Result<Self> {
        let table_name = table_name.into();
        let path = path.into();
        let log_dir = Path::new(&path).join("_delta_log");
        table.update_datafusion_session(session).map_err(|e| {
            QueryError::Execution(format!(
                "Failed to update session for Delta table '{table_name}': {e}"
            ))
        })?;
        let delta_provider = table.table_provider().await.map_err(|e| {
            QueryError::Execution(format!(
                "Failed to build TableProvider for '{table_name}': {e}"
            ))
        })?;
        Ok(Self {
            table_name,
            path,
            log_dir,
            materialized: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            table: Arc::new(tokio::sync::RwLock::new(table)),
            inner_provider: Arc::new(std::sync::RwLock::new(delta_provider)),
        })
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub async fn materialize(&self, session: &dyn Session) -> datafusion::common::Result<()> {
        let current_provider = {
            let guard = self
                .inner_provider
                .read()
                .unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };
        let mem_table = materialize_delta_provider(current_provider, session).await?;
        let _table_guard = self.table.write().await;
        let mut guard = self
            .inner_provider
            .write()
            .unwrap_or_else(|e| e.into_inner());
        *guard = mem_table;
        self.materialized
            .store(true, std::sync::atomic::Ordering::Release);
        info!(
            "Materialized table '{}' into in-memory RecordBatches",
            self.table_name
        );
        Ok(())
    }

    pub async fn reload_if_needed(&self, session: &dyn Session) -> datafusion::common::Result<()> {
        let current_version = {
            let table_guard = self.table.read().await;
            table_guard.version()
        };

        let needs_reload = if let Some(v) = current_version {
            if tokio::fs::metadata(&self.log_dir)
                .await
                .map(|m| m.is_dir())
                .unwrap_or(false)
            {
                let next_commit_file = self.log_dir.join(format!("{:020}.json", v + 1));
                tokio::fs::metadata(&next_commit_file).await.is_ok()
            } else {
                let table_guard = self.table.read().await;
                match table_guard.get_latest_version().await {
                    Ok(latest) => Some(latest) != current_version,
                    Err(e) => {
                        warn!(
                            "Failed to query latest Delta version for '{}': {e}",
                            self.table_name
                        );
                        false
                    }
                }
            }
        } else {
            true
        };

        if needs_reload {
            let mut table_guard = self.table.write().await;
            let still_needs_update = match table_guard.get_latest_version().await {
                Ok(latest) => table_guard.version() != Some(latest),
                Err(_) => false,
            };
            if still_needs_update {
                if let Err(e) = table_guard.update_state().await {
                    warn!(
                        "Failed to update Delta state for '{}': {e}",
                        self.table_name
                    );
                } else {
                    if let Err(e) = table_guard.update_datafusion_session(session) {
                        warn!(
                            "Failed to update DataFusion session for '{}': {e}",
                            self.table_name
                        );
                    }
                    match table_guard.table_provider().await {
                        Ok(new_delta_provider) => {
                            let is_materialized =
                                self.materialized.load(std::sync::atomic::Ordering::Acquire);
                            let provider = if is_materialized {
                                match materialize_delta_provider(
                                    new_delta_provider.clone(),
                                    session,
                                )
                                .await
                                {
                                    Ok(p) => p,
                                    Err(e) => {
                                        warn!(
                                            "Failed to rematerialize batches for '{}': {e}",
                                            self.table_name
                                        );
                                        new_delta_provider
                                    }
                                }
                            } else {
                                new_delta_provider
                            };
                            let mut guard = self
                                .inner_provider
                                .write()
                                .unwrap_or_else(|e| e.into_inner());
                            *guard = provider;
                            info!(
                                "Refreshed Delta table provider for '{}' to version {:?}",
                                self.table_name,
                                table_guard.version()
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to rebuild TableProvider for '{}': {e}",
                                self.table_name
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TableProvider for DynamicDeltaTableProvider {
    fn schema(&self) -> SchemaRef {
        let guard = self
            .inner_provider
            .read()
            .unwrap_or_else(|e| e.into_inner());
        guard.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        let guard = self
            .inner_provider
            .read()
            .unwrap_or_else(|e| e.into_inner());
        guard.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.reload_if_needed(state).await?;
        let provider = {
            let guard = self
                .inner_provider
                .read()
                .unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };
        provider.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        let provider = {
            let guard = self
                .inner_provider
                .read()
                .unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };
        provider.supports_filters_pushdown(filters)
    }
}

pub struct LakehouseCatalog {
    ctx: Arc<SessionContext>,
    config: QueryConfig,
    tables: RwLock<BTreeMap<String, TableSummary>>,
    providers: RwLock<BTreeMap<String, Arc<DynamicDeltaTableProvider>>>,
}

impl LakehouseCatalog {
    pub fn new(ctx: Arc<SessionContext>, config: QueryConfig) -> Self {
        Self {
            ctx,
            config,
            tables: RwLock::new(BTreeMap::new()),
            providers: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn config(&self) -> &QueryConfig {
        &self.config
    }

    pub async fn materialize_table(&self, table_name: &str) -> Result<()> {
        let provider = {
            let guard = self.providers.read().unwrap_or_else(|e| e.into_inner());
            guard.get(table_name).cloned()
        };
        if let Some(dyn_provider) = provider {
            dyn_provider
                .materialize(&self.ctx.state())
                .await
                .map_err(QueryError::DataFusion)?;
            Ok(())
        } else {
            Err(QueryError::Catalog(format!(
                "Table '{table_name}' is not registered as a DynamicDeltaTableProvider"
            )))
        }
    }

    pub async fn register_table(&self, table_name: &str, path: &str) -> Result<()> {
        let url = match deltalake::ensure_table_uri(path) {
            Ok(u) => u,
            Err(e) => {
                let err_msg = format!("Invalid Delta table path '{path}': {e}");
                let mut guard = self.tables.write().unwrap_or_else(|e| e.into_inner());
                guard.insert(
                    table_name.to_string(),
                    TableSummary {
                        name: table_name.to_string(),
                        path: path.to_string(),
                        columns: Vec::new(),
                        status: "error".to_string(),
                        error: Some(err_msg.clone()),
                    },
                );
                return Err(QueryError::Catalog(err_msg));
            }
        };

        let table = match deltalake::open_table(url).await {
            Ok(t) => t,
            Err(e) => {
                let err_msg = format!("Failed to open Delta table '{table_name}' at '{path}': {e}");
                let mut guard = self.tables.write().unwrap_or_else(|e| e.into_inner());
                guard.insert(
                    table_name.to_string(),
                    TableSummary {
                        name: table_name.to_string(),
                        path: path.to_string(),
                        columns: Vec::new(),
                        status: "error".to_string(),
                        error: Some(err_msg.clone()),
                    },
                );
                return Err(QueryError::Delta(err_msg));
            }
        };

        let dynamic_provider =
            DynamicDeltaTableProvider::try_new(table_name, path, table, &self.ctx.state()).await?;

        let schema = dynamic_provider.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|f| ColumnSummary {
                name: f.name().clone(),
                data_type: format!("{}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect();

        let dynamic_provider = Arc::new(dynamic_provider);

        self.ctx
            .register_table(table_name, dynamic_provider.clone())
            .map_err(QueryError::DataFusion)?;

        {
            let mut prov_guard = self.providers.write().unwrap_or_else(|e| e.into_inner());
            prov_guard.insert(table_name.to_string(), dynamic_provider);
        }

        let mut guard = self.tables.write().unwrap_or_else(|e| e.into_inner());
        guard.insert(
            table_name.to_string(),
            TableSummary {
                name: table_name.to_string(),
                path: path.to_string(),
                columns,
                status: "registered".to_string(),
                error: None,
            },
        );

        info!("Registered Delta table '{table_name}' at '{path}'");
        Ok(())
    }

    pub async fn register_all(&self) -> CatalogSummary {
        let data_dir = Path::new(&self.config.data_dir);

        // 1. Process configured tables
        for table_name in &self.config.tables {
            let table_path = data_dir.join(table_name);
            let path_str = table_path.to_string_lossy().to_string();

            if !table_path.exists() {
                warn!("Delta table '{table_name}' at '{path_str}' does not exist; skipping without panic (NFR-5)");
                let mut guard = self.tables.write().unwrap_or_else(|e| e.into_inner());
                guard.insert(
                    table_name.clone(),
                    TableSummary {
                        name: table_name.clone(),
                        path: path_str,
                        columns: Vec::new(),
                        status: "missing".to_string(),
                        error: Some("Table directory does not exist".to_string()),
                    },
                );
                continue;
            }

            if !table_path.join("_delta_log").exists() {
                warn!("Delta table '{table_name}' at '{path_str}' lacks _delta_log transaction directory; skipping without panic (NFR-5)");
                let mut guard = self.tables.write().unwrap_or_else(|e| e.into_inner());
                guard.insert(
                    table_name.clone(),
                    TableSummary {
                        name: table_name.clone(),
                        path: path_str,
                        columns: Vec::new(),
                        status: "uninitialized".to_string(),
                        error: Some("Missing _delta_log directory".to_string()),
                    },
                );
                continue;
            }

            if let Err(e) = self.register_table(table_name, &path_str).await {
                warn!("Failed to register configured Delta table '{table_name}': {e}; continuing (NFR-5)");
            }
        }

        // 2. Discover other delta tables in data_dir (e.g. olist_* or any subfolder with _delta_log)
        if data_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(data_dir) {
                for entry in entries.flatten() {
                    if let Ok(file_type) = entry.file_type() {
                        if file_type.is_dir() {
                            let name = entry.file_name().to_string_lossy().to_string();
                            {
                                let guard = self.tables.read().unwrap_or_else(|e| e.into_inner());
                                if guard.contains_key(&name) {
                                    continue;
                                }
                            }
                            let path_str = entry.path().to_string_lossy().to_string();
                            let has_delta_log = entry.path().join("_delta_log").exists();
                            let is_olist = name.starts_with("olist_");

                            if has_delta_log || is_olist {
                                if !has_delta_log {
                                    warn!("Discovered candidate table '{name}' at '{path_str}' lacks _delta_log; recording as uninitialized (NFR-5)");
                                    let mut guard =
                                        self.tables.write().unwrap_or_else(|e| e.into_inner());
                                    guard.insert(
                                        name.clone(),
                                        TableSummary {
                                            name: name.clone(),
                                            path: path_str,
                                            columns: Vec::new(),
                                            status: "uninitialized".to_string(),
                                            error: Some("Missing _delta_log directory".to_string()),
                                        },
                                    );
                                    continue;
                                }

                                if let Err(e) = self.register_table(&name, &path_str).await {
                                    warn!("Failed to auto-register discovered Delta table '{name}': {e}; continuing (NFR-5)");
                                }
                            }
                        }
                    }
                }
            }
        } else {
            warn!("Configured data_dir '{:?}' does not exist; skipping auto-discovery without panic (NFR-5)", data_dir);
        }

        self.summary()
    }

    pub fn summary(&self) -> CatalogSummary {
        let guard = self.tables.read().unwrap_or_else(|e| e.into_inner());
        let tables: Vec<TableSummary> = guard.values().cloned().collect();
        let registered_count = tables.iter().filter(|t| t.status == "registered").count();
        let missing_count = tables.len() - registered_count;

        CatalogSummary {
            tables,
            registered_count,
            missing_count,
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        let guard = self.tables.read().unwrap_or_else(|e| e.into_inner());
        guard
            .iter()
            .filter(|(_, t)| t.status == "registered")
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn registered_tables_count(&self) -> usize {
        let guard = self.tables.read().unwrap_or_else(|e| e.into_inner());
        guard.values().filter(|t| t.status == "registered").count()
    }

    pub fn get_table(&self, table_name: &str) -> Option<TableSummary> {
        let guard = self.tables.read().unwrap_or_else(|e| e.into_inner());
        guard.get(table_name).cloned()
    }
}
