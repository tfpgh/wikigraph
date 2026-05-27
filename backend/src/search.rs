//! Tantivy title index: build (offline, at image-build time) and query (server).
//!
//! Matching strategy for "find a node" autocomplete:
//!   - the title field is tokenized (whitespace split + lowercase);
//!   - every complete word in the query must match (exact, OR edit-distance-1
//!     fuzzy for words >= 4 chars to tolerate typos);
//!   - the final word is treated as a prefix (the word being typed);
//!   - matches are ordered by `imp` (raw PageRank) descending, so the most
//!     important matching article surfaces first.
//!
//! Ranking is deliberately importance-first rather than BM25 — for a graph
//! navigator "apple" should land on Apple Inc., not an obscure exact match.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, FuzzyTermQuery, Occur, Query, RegexQuery, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, FAST, STORED,
};
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};
use tantivy::{doc, Index, IndexReader, Order, TantivyDocument, Term};

const TOKENIZER: &str = "title";
const FUZZY_MIN_LEN: usize = 4;

#[derive(Deserialize)]
struct InputDoc {
    id: u32,
    t: String,
    x: f64,
    y: f64,
    r: f64,
    imp: f64,
}

#[derive(Serialize)]
pub struct Hit {
    pub id: u32,
    pub t: String,
    pub x: f64,
    pub y: f64,
    pub r: f64,
}

fn register_tokenizer(index: &Index) {
    // Whitespace tokenizer + lowercase, used both at index and query time so
    // the terms line up.
    let analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .build();
    index.tokenizers().register(TOKENIZER, analyzer);
}

/// Build the index from the JSONL exported by `offline/build_search_docs.py`.
/// Called by the `build_index` binary during `docker build`.
pub fn build(index_dir: &Path, docs_path: &Path) -> Result<()> {
    std::fs::create_dir_all(index_dir)?;

    let mut sb = Schema::builder();
    let title_indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER)
        .set_index_option(IndexRecordOption::WithFreqs);
    let title_opts = TextOptions::default()
        .set_indexing_options(title_indexing)
        .set_stored();
    let title = sb.add_text_field("title", title_opts);
    let id = sb.add_u64_field("id", STORED | FAST);
    let x = sb.add_f64_field("x", STORED);
    let y = sb.add_f64_field("y", STORED);
    let r = sb.add_f64_field("r", STORED);
    let imp = sb.add_f64_field("imp", FAST);
    let schema = sb.build();

    let index = Index::create_in_dir(index_dir, schema)?;
    register_tokenizer(&index);

    let mut writer = index.writer(512_000_000)?; // 512 MB indexing heap

    let file = File::open(docs_path).with_context(|| format!("open {}", docs_path.display()))?;
    let mut count = 0u64;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let d: InputDoc = serde_json::from_str(&line)?;
        writer.add_document(doc!(
            title => d.t,
            id => d.id as u64,
            x => d.x,
            y => d.y,
            r => d.r,
            imp => d.imp,
        ))?;
        count += 1;
    }
    writer.commit()?;
    tracing::info!(docs = count, "search index built");
    Ok(())
}

pub struct Search {
    reader: IndexReader,
    title: Field,
    id: Field,
    x: Field,
    y: Field,
    r: Field,
}

impl Search {
    pub fn open(index_dir: &Path) -> Result<Search> {
        let index = Index::open_in_dir(index_dir)
            .with_context(|| format!("open index {}", index_dir.display()))?;
        register_tokenizer(&index);
        let schema = index.schema();
        Ok(Search {
            reader: index.reader()?,
            title: schema.get_field("title")?,
            id: schema.get_field("id")?,
            x: schema.get_field("x")?,
            y: schema.get_field("y")?,
            r: schema.get_field("r")?,
        })
    }

    pub fn query(&self, raw: &str, limit: usize) -> Result<Vec<Hit>> {
        let Some(query) = self.build_query(raw) else {
            return Ok(Vec::new());
        };
        let searcher = self.reader.searcher();
        // Order matches by importance (PageRank) rather than text score.
        let collector = TopDocs::with_limit(limit).order_by_fast_field::<f64>("imp", Order::Desc);
        let top = searcher.search(&*query, &collector)?;

        let mut hits = Vec::with_capacity(top.len());
        for (_imp, addr) in top {
            let doc: TantivyDocument = searcher.doc(addr)?;
            hits.push(Hit {
                id: doc.get_first(self.id).and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                t: doc
                    .get_first(self.title)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                x: doc
                    .get_first(self.x)
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                y: doc
                    .get_first(self.y)
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                r: doc
                    .get_first(self.r)
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
            });
        }
        Ok(hits)
    }

    fn build_query(&self, raw: &str) -> Option<Box<dyn Query>> {
        let tokens: Vec<String> = raw
            .to_lowercase()
            .split_whitespace()
            .map(str::to_string)
            .collect();
        if tokens.is_empty() {
            return None;
        }

        let last = tokens.len() - 1;
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(tokens.len());
        for (i, tok) in tokens.iter().enumerate() {
            if i == last {
                // The word being typed: prefix match via an anchored regex over
                // the term dictionary, falling back to an exact term.
                let pattern = format!("{}.*", regex::escape(tok));
                let clause: Box<dyn Query> = match RegexQuery::from_pattern(&pattern, self.title) {
                    Ok(q) => Box::new(q),
                    Err(_) => Box::new(TermQuery::new(
                        Term::from_field_text(self.title, tok),
                        IndexRecordOption::Basic,
                    )),
                };
                clauses.push((Occur::Must, clause));
            } else {
                // A complete word: exact, OR fuzzy for longer words.
                let term = Term::from_field_text(self.title, tok);
                let exact: Box<dyn Query> =
                    Box::new(TermQuery::new(term.clone(), IndexRecordOption::Basic));
                let clause: Box<dyn Query> = if tok.chars().count() >= FUZZY_MIN_LEN {
                    let fuzzy = FuzzyTermQuery::new(term, 1, true);
                    Box::new(BooleanQuery::new(vec![
                        (Occur::Should, exact),
                        (Occur::Should, Box::new(fuzzy)),
                    ]))
                } else {
                    exact
                };
                clauses.push((Occur::Must, clause));
            }
        }
        Some(Box::new(BooleanQuery::new(clauses)))
    }
}
