//! Directed graph in CSR form + bidirectional BFS for shortest paths.
//!
//! Loads the flat binary written by `offline/build_graph_csr.py` (see that file
//! for the layout) into two adjacency arrays — forward (out-edges) and reverse
//! (in-edges) — and runs a bidirectional BFS that expands the smaller frontier
//! each round. On a small-world graph like Wikipedia the frontiers meet after
//! touching a tiny fraction of the nodes, so each query is milliseconds.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

const MAGIC: &[u8; 4] = b"WGCS";

#[derive(Clone, Copy)]
enum Dir {
    Out,
    In,
}

pub struct Graph {
    n_nodes: u32,
    fwd_off: Vec<u32>,
    fwd_nbr: Vec<u32>,
    rev_off: Vec<u32>,
    rev_nbr: Vec<u32>,
}

impl Graph {
    pub fn load(path: &Path) -> Result<Graph> {
        let mut r =
            BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);

        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if &magic != MAGIC {
            bail!("bad magic in {}", path.display());
        }
        let version = read_u32(&mut r)?;
        if version != 1 {
            bail!("unsupported csr version {version}");
        }
        let n_nodes = read_u32(&mut r)?;
        let n_edges = read_u32(&mut r)?;

        let fwd_off = read_u32_vec(&mut r, n_nodes as usize + 1)?;
        let fwd_nbr = read_u32_vec(&mut r, n_edges as usize)?;
        let rev_off = read_u32_vec(&mut r, n_nodes as usize + 1)?;
        let rev_nbr = read_u32_vec(&mut r, n_edges as usize)?;

        Ok(Graph {
            n_nodes,
            fwd_off,
            fwd_nbr,
            rev_off,
            rev_nbr,
        })
    }

    pub fn n_nodes(&self) -> u32 {
        self.n_nodes
    }

    fn neighbors(&self, dir: Dir, u: u32) -> &[u32] {
        let (off, nbr) = match dir {
            Dir::Out => (&self.fwd_off, &self.fwd_nbr),
            Dir::In => (&self.rev_off, &self.rev_nbr),
        };
        let s = off[u as usize] as usize;
        let e = off[u as usize + 1] as usize;
        &nbr[s..e]
    }

    /// Directed shortest path `src` -> `dst` (inclusive of both), or None if
    /// unreachable. Returns `[src]` when src == dst.
    pub fn shortest_path(&self, src: u32, dst: u32) -> Option<Vec<u32>> {
        if src >= self.n_nodes || dst >= self.n_nodes {
            return None;
        }
        if src == dst {
            return Some(vec![src]);
        }

        // (parent, depth) per visited node, one map per direction. A HashMap
        // keyed only on visited nodes is cheaper than zeroing an N-sized array
        // per request and is trivially safe to build concurrently.
        let mut fwd: HashMap<u32, (u32, u32)> = HashMap::new();
        let mut bwd: HashMap<u32, (u32, u32)> = HashMap::new();
        fwd.insert(src, (src, 0));
        bwd.insert(dst, (dst, 0));
        let mut ff = vec![src];
        let mut bf = vec![dst];
        let (mut fd, mut bd) = (0u32, 0u32);

        while !ff.is_empty() && !bf.is_empty() {
            // Expand whichever side has the smaller frontier — keeps total work
            // near the geometric mean of the two BFS trees.
            if ff.len() <= bf.len() {
                fd += 1;
                if let Some(meet) = self.expand(&mut ff, &mut fwd, &bwd, Dir::Out, fd) {
                    return Some(self.build_path(meet, &fwd, &bwd));
                }
            } else {
                bd += 1;
                if let Some(meet) = self.expand(&mut bf, &mut bwd, &fwd, Dir::In, bd) {
                    return Some(self.build_path(meet, &fwd, &bwd));
                }
            }
        }
        None
    }

    /// Expand one full BFS level. Returns the meeting node with the smallest
    /// combined depth if this level touches the other side. Completing the
    /// level (rather than returning on the first touch) keeps the path shortest.
    fn expand(
        &self,
        frontier: &mut Vec<u32>,
        this: &mut HashMap<u32, (u32, u32)>,
        other: &HashMap<u32, (u32, u32)>,
        dir: Dir,
        depth: u32,
    ) -> Option<u32> {
        let mut next = Vec::new();
        let mut best: Option<(u32, u32)> = None; // (meet node, combined depth)
        for &u in frontier.iter() {
            for &v in self.neighbors(dir, u) {
                if !this.contains_key(&v) {
                    this.insert(v, (u, depth));
                    if let Some(&(_, other_depth)) = other.get(&v) {
                        let combined = depth + other_depth;
                        if best.map_or(true, |(_, b)| combined < b) {
                            best = Some((v, combined));
                        }
                    }
                    next.push(v);
                }
            }
        }
        *frontier = next;
        best.map(|(node, _)| node)
    }

    fn build_path(
        &self,
        meet: u32,
        fwd: &HashMap<u32, (u32, u32)>,
        bwd: &HashMap<u32, (u32, u32)>,
    ) -> Vec<u32> {
        // meet -> src via forward parents, then reverse to get src..meet.
        let mut path = vec![meet];
        let mut cur = meet;
        while let Some(&(parent, _)) = fwd.get(&cur) {
            if parent == cur {
                break;
            }
            path.push(parent);
            cur = parent;
        }
        path.reverse();

        // meet -> dst via backward parents (meet already in path).
        let mut cur = meet;
        while let Some(&(parent, _)) = bwd.get(&cur) {
            if parent == cur {
                break;
            }
            path.push(parent);
            cur = parent;
        }
        path
    }
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u32_vec<R: Read>(r: &mut R, len: usize) -> Result<Vec<u32>> {
    // Vec<u32> is 4-byte aligned, so casting to &mut [u8] for read_exact is
    // sound. The file is little-endian; assumes an LE host (true on the
    // container's x86_64/arm64).
    let mut v = vec![0u32; len];
    r.read_exact(bytemuck::cast_slice_mut(&mut v))?;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Directed edges: 0->1, 1->2, 2->3, 0->4, 4->3, 3->5.
    // Two routes from 0 to 3: 0->4->3 (short) and 0->1->2->3 (long).
    fn sample() -> Graph {
        Graph {
            n_nodes: 6,
            fwd_off: vec![0, 2, 3, 4, 5, 6, 6],
            fwd_nbr: vec![1, 4, 2, 3, 5, 3],
            rev_off: vec![0, 0, 1, 2, 4, 5, 6],
            rev_nbr: vec![0, 1, 2, 4, 0, 3],
        }
    }

    #[test]
    fn picks_shortest_of_two_routes() {
        assert_eq!(sample().shortest_path(0, 3), Some(vec![0, 4, 3]));
    }

    #[test]
    fn extends_through_to_a_deeper_target() {
        assert_eq!(sample().shortest_path(0, 5), Some(vec![0, 4, 3, 5]));
    }

    #[test]
    fn single_hop_chain() {
        assert_eq!(sample().shortest_path(0, 2), Some(vec![0, 1, 2]));
    }

    #[test]
    fn same_node_is_trivial() {
        assert_eq!(sample().shortest_path(3, 3), Some(vec![3]));
    }

    #[test]
    fn respects_direction_unreachable() {
        // No edges lead back to 0, so nothing reaches it.
        assert_eq!(sample().shortest_path(5, 0), None);
    }

    #[test]
    fn out_of_range_ids() {
        assert_eq!(sample().shortest_path(0, 99), None);
    }
}
