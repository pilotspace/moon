use std::collections::HashMap;
use crate::error::Result;
use crate::types::WorkspaceInfo;
use crate::util::parse_flat_kv;

/// Workspace sub-client (`WS.*` commands).
///
/// Workspaces provide transparent key-namespace isolation. After calling
/// [`WorkspaceClient::auth`], all subsequent commands on this connection use
/// a `{ws_hex}:` hash-tag prefix, routing all keys to the same shard.
///
/// Obtain via [`MoonClient::workspace`](crate::MoonClient::workspace).
pub struct WorkspaceClient {
    pub(crate) conn: redis::aio::MultiplexedConnection,
}

impl WorkspaceClient {
    /// Create a new workspace and return its UUID-v7 ID.
    pub async fn create(&mut self, name: &str) -> Result<String> {
        Ok(redis::cmd("WS").arg("CREATE").arg(name).query_async(&mut self.conn).await?)
    }

    /// Drop a workspace and all its data.
    pub async fn drop(&mut self, workspace_id: &str) -> Result<()> {
        redis::cmd("WS").arg("DROP").arg(workspace_id)
            .query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    /// Bind the current connection to a workspace. After this call, all subsequent
    /// commands on this connection are transparently scoped to the workspace.
    pub async fn auth(&mut self, workspace_id: &str) -> Result<()> {
        redis::cmd("WS").arg("AUTH").arg(workspace_id)
            .query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    /// Retrieve workspace metadata.
    pub async fn info(&mut self, workspace_id: &str) -> Result<WorkspaceInfo> {
        let raw: redis::Value = redis::cmd("WS").arg("INFO").arg(workspace_id)
            .query_async(&mut self.conn).await?;
        Ok(parse_ws_info(workspace_id, raw))
    }

    /// List all workspace IDs.
    pub async fn list(&mut self) -> Result<Vec<String>> {
        Ok(redis::cmd("WS").arg("LIST").query_async(&mut self.conn).await?)
    }
}

fn parse_ws_info(id: &str, raw: redis::Value) -> WorkspaceInfo {
    let arr = match raw {
        redis::Value::Array(a) => a,
        _ => return WorkspaceInfo {
            id: id.to_string(),
            name: String::new(),
            created_at: 0,
            extra: HashMap::new(),
        },
    };
    let kv = parse_flat_kv(&arr);
    WorkspaceInfo {
        id: id.to_string(),
        name: kv.get("name").cloned().unwrap_or_default(),
        created_at: kv.get("created_at").and_then(|v| v.parse().ok()).unwrap_or(0),
        extra: kv.into_iter()
            .filter(|(k, _)| !matches!(k.as_str(), "name" | "created_at"))
            .collect(),
    }
}
