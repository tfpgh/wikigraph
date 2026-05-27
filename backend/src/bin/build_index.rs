//! Build the Tantivy title index from the exported JSONL. Run during the
//! Docker build so the index is written by the same tantivy version the server
//! reads.
//!
//! Usage: build_index <search_docs.jsonl> <index_dir>

use std::path::PathBuf;

use wikigraph_backend::search;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let mut args = std::env::args().skip(1);
    let docs = args
        .next()
        .expect("usage: build_index <search_docs.jsonl> <index_dir>");
    let index_dir = args
        .next()
        .expect("usage: build_index <search_docs.jsonl> <index_dir>");

    search::build(&PathBuf::from(index_dir), &PathBuf::from(docs))?;
    Ok(())
}
