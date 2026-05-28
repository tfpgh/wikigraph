//! Tantivy title index: build (offline, at image-build time) and query (server).
//!
//! Matching ("find a node" autocomplete):
//!   - tokens are whitespace-split, lowercased, and ASCII-folded (so `beyonce`
//!     finds *Beyoncé*); the query is run through the same analyzers so the
//!     index and query normalize identically;
//!   - every complete word must match exact, or fuzzy with an edit budget
//!     scaled by length (none <4, 1 for 4-7, 2 for 8+), so "phsycology"
//!     reaches *Psychology*;
//!   - the final word — the one being typed — matches as a prefix OR fuzzy;
//!   - small English connector words (and, or, of, the, ...) are demoted from
//!     MUST to SHOULD when a real token is also present, so "lord of the
//!     rings" still matches a title that only tokenizes to [lord, rings];
//!   - a single-character query short-circuits to titles whose first char is
//!     the typed char (regex on `title_raw`), avoiding a word-level prefix
//!     scan that would enumerate millions of "a*" tokens.
//!
//! Ranking is tiered so an exact title always wins, regardless of PageRank
//! (typing "SoFi" must return *SoFi* even though it's a low-PageRank page):
//!   - the token match above is the recall filter (MUST);
//!   - boosted SHOULD clauses reward an exact title (x1000), a title that
//!     starts with the query (x100), and an in-order phrase match with small
//!     slop (x10) — so "world war" promotes *World War II* over *War of the
//!     Worlds*;
//!   - `tweak_score` then multiplies the base score by `1 + 4*imp`, where
//!     `imp` is importance (PageRank) normalized to [0, 1]. Importance only
//!     orders results *within* a tier; the 10x gap between tiers stays
//!     wider than the <=5x importance multiplier.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use tantivy::collector::TopDocs;
use tantivy::indexer::NoMergePolicy;
use tantivy::query::{
    BooleanQuery, BoostQuery, FuzzyTermQuery, Occur, PhraseQuery, Query, RegexQuery, TermQuery,
};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, FAST, STORED,
};
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, RawTokenizer, SimpleTokenizer, TextAnalyzer,
};
use tantivy::{doc, DocId, Index, IndexReader, Score, SegmentReader, TantivyDocument, Term};

const TOKENIZER: &str = "title"; // word-split + lowercase + ascii-fold
const RAW_TOKENIZER: &str = "title_raw"; // whole title as one token, normalized

// Tier boosts. Exact > starts-with > phrase > plain token match. Each gap is
// 10x, which stays wider than the <=5x importance multiplier so importance
// can't reorder across tiers.
const EXACT_BOOST: Score = 1000.0;
const PREFIX_BOOST: Score = 100.0;
const PHRASE_BOOST: Score = 10.0;
// imp is normalized to [0, 1]; weight 4.0 => multiplier in [1, 5].
const IMPORTANCE_WEIGHT: f64 = 4.0;

/// Edit-distance budget for a token, scaled by length. Short tokens get no
/// fuzzy (every char carries too much signal); long tokens get 2 to catch
/// double-typos like "phsycology" -> "psychology". None means exact-only.
fn fuzzy_distance(tok: &str) -> Option<u8> {
    match tok.chars().count() {
        0..=3 => None,
        4..=7 => Some(1),
        _ => Some(2),
    }
}

/// English function words that get demoted from MUST to SHOULD when other
/// non-connector tokens are present in the query. Lets "lord of the rings"
/// match a title whose tokens are just [lord, rings], without losing the
/// ability to find a title literally called "The" or "And".
const CONNECTORS: &[&str] = &[
    "and", "or", "of", "the", "a", "an", "in", "on", "at", "for", "to",
];

fn is_connector(tok: &str) -> bool {
    CONNECTORS.contains(&tok)
}

#[derive(Deserialize)]
struct InputDoc {
    id: u32,
    t: String,
    x: f64,
    y: f64,
    r: f64,
    cl: u32,
    imp: f64,
}

#[derive(Serialize)]
pub struct Hit {
    pub id: u32,
    pub t: String,
    pub x: f64,
    pub y: f64,
    pub r: f64,
    pub cl: u32,
}

fn register_tokenizers(index: &Index) {
    // Word-level tokenizer for token/fuzzy/prefix matching.
    let words = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build();
    index.tokenizers().register(TOKENIZER, words);

    // Whole-title-as-one-term tokenizer, same normalization, for exact and
    // starts-with matching on the full title.
    let raw = TextAnalyzer::builder(RawTokenizer::default())
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build();
    index.tokenizers().register(RAW_TOKENIZER, raw);
}

/// Build the index from the JSONL exported by `offline/build_search_docs.py`.
/// Called by the `build_index` binary during `docker build`. `imp` in the JSONL
/// is raw PageRank; it's log-min-max normalized to [0, 1] here so the JSONL
/// format (and the cluster pipeline) stays unchanged when ranking is tuned.
pub fn build(index_dir: &Path, docs_path: &Path) -> Result<()> {
    std::fs::create_dir_all(index_dir)?;

    let mut sb = Schema::builder();
    let title_indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let title_opts = TextOptions::default()
        .set_indexing_options(title_indexing)
        .set_stored();
    let title = sb.add_text_field("title", title_opts);

    let raw_indexing = TextFieldIndexing::default()
        .set_tokenizer(RAW_TOKENIZER)
        .set_index_option(IndexRecordOption::Basic);
    let title_raw =
        sb.add_text_field("title_raw", TextOptions::default().set_indexing_options(raw_indexing));

    let id = sb.add_u64_field("id", STORED | FAST);
    let x = sb.add_f64_field("x", STORED);
    let y = sb.add_f64_field("y", STORED);
    let r = sb.add_f64_field("r", STORED);
    let cl = sb.add_u64_field("cl", STORED);
    let imp = sb.add_f64_field("imp", FAST);
    let schema = sb.build();

    let index = Index::create_in_dir(index_dir, schema)?;
    register_tokenizers(&index);
    let mut writer = index.writer(512_000_000)?; // 512 MB indexing heap
    // Disable auto-merge so our explicit force-merge below doesn't race with
    // background merges already consuming the same segments.
    writer.set_merge_policy(Box::new(NoMergePolicy));

    // Pass 1: importance range on a log scale (PageRank is heavy-tailed).
    let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
    for_each_doc(docs_path, |d| {
        let l = d.imp.max(1e-12).ln();
        lo = lo.min(l);
        hi = hi.max(l);
    })?;
    let span = (hi - lo).max(1e-9);

    // Pass 2: index with normalized importance.
    let mut count = 0u64;
    for_each_doc(docs_path, |d| {
        let imp_norm = (((d.imp.max(1e-12).ln()) - lo) / span).clamp(0.0, 1.0);
        let _ = writer.add_document(doc!(
            title => d.t.clone(),
            title_raw => d.t,
            id => d.id as u64,
            x => d.x,
            y => d.y,
            r => d.r,
            cl => d.cl as u64,
            imp => imp_norm,
        ));
        count += 1;
    })?;
    writer.commit()?;

    // Force-merge to a single segment. The index is built once at image-build
    // time and never written again, so the default lazy merge policy leaves
    // free perf on the table — one segment means one term dictionary lookup
    // per query and ~20-30% smaller on disk.
    let segment_ids = index.searchable_segment_ids()?;
    if segment_ids.len() > 1 {
        writer.merge(&segment_ids).wait()?;
    }
    writer.wait_merging_threads()?;

    tracing::info!(docs = count, "search index built");
    Ok(())
}

fn for_each_doc(path: &Path, mut f: impl FnMut(InputDoc)) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        f(serde_json::from_str(&line)?);
    }
    Ok(())
}

pub struct Search {
    reader: IndexReader,
    title: Field,
    title_raw: Field,
    id: Field,
    x: Field,
    y: Field,
    r: Field,
    cl: Field,
    word_analyzer: TextAnalyzer,
    raw_analyzer: TextAnalyzer,
}

impl Search {
    pub fn open(index_dir: &Path) -> Result<Search> {
        let index = Index::open_in_dir(index_dir)
            .with_context(|| format!("open index {}", index_dir.display()))?;
        register_tokenizers(&index);
        let schema = index.schema();
        let word_analyzer = index
            .tokenizers()
            .get(TOKENIZER)
            .expect("title tokenizer registered");
        let raw_analyzer = index
            .tokenizers()
            .get(RAW_TOKENIZER)
            .expect("raw tokenizer registered");
        Ok(Search {
            reader: index.reader()?,
            title: schema.get_field("title")?,
            title_raw: schema.get_field("title_raw")?,
            id: schema.get_field("id")?,
            x: schema.get_field("x")?,
            y: schema.get_field("y")?,
            r: schema.get_field("r")?,
            cl: schema.get_field("cl")?,
            word_analyzer,
            raw_analyzer,
        })
    }

    pub fn query(&self, raw: &str, limit: usize) -> Result<Vec<Hit>> {
        let Some(query) = self.build_query(raw) else {
            return Ok(Vec::new());
        };
        let searcher = self.reader.searcher();

        // base relevance (tier boosts) x (1 + weight * normalized importance)
        let collector = TopDocs::with_limit(limit).tweak_score(move |seg: &SegmentReader| {
            let imp = seg.fast_fields().f64("imp").ok();
            move |doc: DocId, score: Score| -> f64 {
                let i = imp.as_ref().and_then(|c| c.first(doc)).unwrap_or(0.0);
                (score as f64) * (1.0 + IMPORTANCE_WEIGHT * i)
            }
        });
        let top = searcher.search(&*query, &collector)?;

        let mut hits = Vec::with_capacity(top.len());
        for (_score, addr) in top {
            let doc: TantivyDocument = searcher.doc(addr)?;
            hits.push(Hit {
                id: doc.get_first(self.id).and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                t: doc
                    .get_first(self.title)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                x: doc.get_first(self.x).and_then(|v| v.as_f64()).unwrap_or(0.0),
                y: doc.get_first(self.y).and_then(|v| v.as_f64()).unwrap_or(0.0),
                r: doc.get_first(self.r).and_then(|v| v.as_f64()).unwrap_or(0.0),
                cl: doc.get_first(self.cl).and_then(|v| v.as_u64()).unwrap_or(0) as u32,
            });
        }
        Ok(hits)
    }

    /// Normalize text through an analyzer, returning its tokens in order.
    fn analyze(&self, analyzer: &TextAnalyzer, text: &str) -> Vec<String> {
        let mut analyzer = analyzer.clone();
        let mut stream = analyzer.token_stream(text);
        let mut out = Vec::new();
        while stream.advance() {
            out.push(stream.token().text.clone());
        }
        out
    }

    fn build_query(&self, raw: &str) -> Option<Box<dyn Query>> {
        let tokens = self.analyze(&self.word_analyzer, raw);
        if tokens.is_empty() {
            return None;
        }

        // 1-char query: match titles that *begin* with the char (regex on the
        // whole-title field), not titles containing any word starting with it.
        // A word-level "a.*" would enumerate millions of terms and yield a
        // candidate set too noisy to rank cleanly; the title_raw prefix path
        // keeps the set small and ordering up to PageRank via tweak_score.
        if tokens.len() == 1 && tokens[0].chars().count() == 1 {
            let tok = &tokens[0];
            let rx = RegexQuery::from_pattern(
                &format!("{}.*", regex::escape(tok)),
                self.title_raw,
            )
            .ok()?;
            let exact = TermQuery::new(
                Term::from_field_text(self.title_raw, tok),
                IndexRecordOption::Basic,
            );
            return Some(Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(rx)),
                (
                    Occur::Should,
                    Box::new(BoostQuery::new(Box::new(exact), EXACT_BOOST)),
                ),
            ])));
        }

        // Recall: every word must match (last word as the prefix being typed).
        // Connector words demote to SHOULD when a real token is also present
        // so queries with extra fillers ("lord of the rings") still match
        // titles whose tokens skip the fillers.
        let last = tokens.len() - 1;
        let has_non_connector = tokens.iter().any(|t| !is_connector(t));
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        for (i, tok) in tokens.iter().enumerate() {
            let (occur, clause) = if i == last {
                (Occur::Must, self.prefix_or_fuzzy(tok))
            } else if has_non_connector && is_connector(tok) {
                (Occur::Should, self.exact_or_fuzzy(tok))
            } else {
                (Occur::Must, self.exact_or_fuzzy(tok))
            };
            clauses.push((occur, clause));
        }

        // Boosts: reward exact / starts-with on the normalized whole title.
        if let Some(full) = self.analyze(&self.raw_analyzer, raw).into_iter().next() {
            let exact = TermQuery::new(
                Term::from_field_text(self.title_raw, &full),
                IndexRecordOption::Basic,
            );
            clauses.push((
                Occur::Should,
                Box::new(BoostQuery::new(Box::new(exact), EXACT_BOOST)),
            ));
            if let Ok(rx) =
                RegexQuery::from_pattern(&format!("{}.*", regex::escape(&full)), self.title_raw)
            {
                clauses.push((
                    Occur::Should,
                    Box::new(BoostQuery::new(Box::new(rx), PREFIX_BOOST)),
                ));
            }
        }

        // Phrase proximity: when there are 2+ tokens, reward in-order matches
        // (small slop tolerates a stop-word or comma between them). Separates
        // "World War II" from "War of the Worlds" on a "world war" query.
        if tokens.len() >= 2 {
            let terms: Vec<Term> = tokens
                .iter()
                .map(|t| Term::from_field_text(self.title, t))
                .collect();
            let mut phrase = PhraseQuery::new(terms);
            phrase.set_slop(2);
            clauses.push((
                Occur::Should,
                Box::new(BoostQuery::new(Box::new(phrase), PHRASE_BOOST)),
            ));
        }

        Some(Box::new(BooleanQuery::new(clauses)))
    }

    /// A completed word: exact, OR length-scaled fuzzy (see `fuzzy_distance`).
    fn exact_or_fuzzy(&self, tok: &str) -> Box<dyn Query> {
        let term = Term::from_field_text(self.title, tok);
        let exact: Box<dyn Query> =
            Box::new(TermQuery::new(term.clone(), IndexRecordOption::WithFreqs));
        if let Some(d) = fuzzy_distance(tok) {
            Box::new(BooleanQuery::new(vec![
                (Occur::Should, exact),
                (Occur::Should, Box::new(FuzzyTermQuery::new(term, d, true))),
            ]))
        } else {
            exact
        }
    }

    /// The word being typed: prefix, OR length-scaled fuzzy (typo tolerance
    /// mid-type).
    fn prefix_or_fuzzy(&self, tok: &str) -> Box<dyn Query> {
        let mut subs: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        if let Ok(rx) = RegexQuery::from_pattern(&format!("{}.*", regex::escape(tok)), self.title) {
            subs.push((Occur::Should, Box::new(rx)));
        }
        if let Some(d) = fuzzy_distance(tok) {
            subs.push((
                Occur::Should,
                Box::new(FuzzyTermQuery::new(
                    Term::from_field_text(self.title, tok),
                    d,
                    true,
                )),
            ));
        }
        if subs.is_empty() {
            Box::new(TermQuery::new(
                Term::from_field_text(self.title, tok),
                IndexRecordOption::WithFreqs,
            ))
        } else {
            Box::new(BooleanQuery::new(subs))
        }
    }
}
