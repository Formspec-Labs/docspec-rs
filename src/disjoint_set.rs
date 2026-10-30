//! Shared union-find primitive; callers own keys and application policy.
// Rust guideline compliant 2026-02-21
/// Union-find (disjoint-set) forest with path halving and union by rank.
pub(crate) struct DisjointSet {
    parent: Vec<u32>,
    rank: Vec<u8>,
}

impl DisjointSet {
    pub(crate) fn new(n: usize) -> Self {
        let count = u32::try_from(n).expect("disjoint set size fits u32");
        Self {
            parent: (0..count).collect(),
            rank: vec![0; n],
        }
    }

    pub(crate) fn push(&mut self) -> usize {
        let idx = self.parent.len();
        self.parent
            .push(u32::try_from(idx).expect("disjoint set index fits u32"));
        self.rank.push(0);
        idx
    }

    pub(crate) fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] as usize != x {
            let p = self.parent[x] as usize;
            self.parent[x] = self.parent[p];
            x = p;
        }
        x
    }

    pub(crate) fn union(&mut self, a: usize, b: usize) -> usize {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return ra;
        }
        if self.rank[ra] < self.rank[rb] {
            self.parent[ra] = rb as u32;
            rb
        } else if self.rank[ra] > self.rank[rb] {
            self.parent[rb] = ra as u32;
            ra
        } else {
            self.parent[rb] = ra as u32;
            self.rank[ra] = self.rank[ra].saturating_add(1);
            ra
        }
    }
}

impl DisjointSet {
    pub(crate) fn try_reserve_nodes(
        &mut self,
        target: usize,
    ) -> Result<(), std::collections::TryReserveError> {
        self.parent.try_reserve_exact(target - self.parent.len())?;
        self.rank.try_reserve_exact(target - self.rank.len())
    }
    pub(crate) fn storage_bytes(&self) -> usize {
        self.parent.capacity() * std::mem::size_of::<u32>() + self.rank.capacity()
    }
}
