//! HTTP service: title search + directed shortest path over the in-RAM graph.
//!
//! Loads the CSR graph and Tantivy index (both baked into the image at
//! DATA_DIR) once at startup, shares them read-only via Arc, and runs the
//! CPU-bound work on the blocking pool so the async runtime stays responsive.
//! Caching/CORS live in the Cloudflare Worker in front of this.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};

use wikigraph_backend::graph::Graph;
use wikigraph_backend::search::{Hit, Search};

struct AppState {
    graph: Graph,
    search: Search,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/opt/data".into()));

    tracing::info!("loading graph from {}", data.join("graph.csr").display());
    let graph = Graph::load(&data.join("graph.csr"))?;
    tracing::info!(n_nodes = graph.n_nodes(), "graph loaded");

    tracing::info!("opening search index");
    let search = Search::open(&data.join("index"))?;

    let state = Arc::new(AppState { graph, search });
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/search", get(search_handler))
        .route("/path", get(path_handler))
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    #[serde(default = "default_n")]
    n: usize,
}

fn default_n() -> usize {
    10
}

async fn search_handler(
    State(st): State<Arc<AppState>>,
    Query(p): Query<SearchParams>,
) -> impl IntoResponse {
    let n = p.n.clamp(1, 50);
    let res = tokio::task::spawn_blocking(move || st.search.query(&p.q, n)).await;
    match res {
        Ok(Ok(hits)) => Json(hits).into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
struct PathParams {
    from: u32,
    to: u32,
}

#[derive(Serialize)]
struct PathResponse {
    found: bool,
    length: usize,
    path: Vec<Hit>,
}

async fn path_handler(
    State(st): State<Arc<AppState>>,
    Query(p): Query<PathParams>,
) -> impl IntoResponse {
    let res = tokio::task::spawn_blocking(move || {
        let Some(ids) = st.graph.shortest_path(p.from, p.to) else {
            return Ok::<Option<Vec<Hit>>, anyhow::Error>(None);
        };
        Ok(Some(st.search.lookup_ids(&ids)?))
    })
    .await;
    match res {
        Ok(Ok(Some(path))) => Json(PathResponse {
            found: true,
            length: path.len(),
            path,
        })
        .into_response(),
        Ok(Ok(None)) => Json(PathResponse {
            found: false,
            length: 0,
            path: Vec::new(),
        })
        .into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
