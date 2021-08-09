use dashmap::DashMap;
use std::hash::Hash;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct ConcurrentDisjointSet<T: Hash + Eq> {
    val_to_index: DashMap<T, usize>,
    parents: Vec<AtomicUsize>,
    next_index: AtomicUsize,
    capacity: usize,
}

impl<T: Hash + Eq> ConcurrentDisjointSet<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let mut parents = Vec::with_capacity(capacity);
        for i in 0..capacity {
            parents.push(AtomicUsize::new(i));
        }
        Self {
            val_to_index: DashMap::with_capacity(capacity),
            parents,
            next_index: AtomicUsize::new(0),
            capacity,
        }
    }

    pub fn add(&self, x: T) -> Result<bool, DisjointSetError> {
        if self.val_to_index.contains_key(&x) {
            return Ok(false);
        }
        let next_index = self.next_index.fetch_add(1, Ordering::SeqCst);
        if next_index >= self.capacity {
            return Err(DisjointSetError::Full);
        }
        self.val_to_index.insert(x, next_index);
        Ok(true)
    }

    pub fn is_empty(&self) -> bool {
        self.val_to_index.is_empty()
    }

    pub fn len(&self) -> usize {
        self.val_to_index.len()
    }

    fn find(&self, x: usize) -> usize {
        let mut curr = x;
        loop {
            let parent = self.parents[curr].load(Ordering::SeqCst);
            if parent == curr {
                break;
            } else {
                curr = parent;
            }
        }
        curr
    }

    pub fn union(&self, x: &T, y: &T) -> Result<(), DisjointSetError> {
        let x = *self
            .val_to_index
            .get(x)
            .ok_or(DisjointSetError::MissingElement)?;
        let y = *self
            .val_to_index
            .get(y)
            .ok_or(DisjointSetError::MissingElement)?;
        let sx = self.find(x);
        let sy = self.find(y);

        // x and y are already in the same set, no work needed
        if sx != sy {
            self.parents[sy].store(sx, Ordering::SeqCst);
        }
        Ok(())
    }

    pub fn same_set(&self, x: &T, y: &T) -> Result<bool, DisjointSetError> {
        let x = *self
            .val_to_index
            .get(x)
            .ok_or(DisjointSetError::MissingElement)?;
        let y = *self
            .val_to_index
            .get(y)
            .ok_or(DisjointSetError::MissingElement)?;
        let sx = self.find(x);
        let sy = self.find(y);

        Ok(sx == sy)
    }
}

#[derive(Debug)]
pub enum DisjointSetError {
    MissingElement,
    Full,
}

#[cfg(test)]
mod tests {
    use super::*;
    //use std::sync::{Arc, Barrier};
    //use std::thread;

    #[test]
    fn test_union_and_same_set() {
        let ds = ConcurrentDisjointSet::with_capacity(8);
        for i in 0..8 {
            ds.add(i).unwrap();
        }
        assert!(!ds.same_set(&0, &2).unwrap());
        assert!(!ds.same_set(&0, &2).unwrap());
        assert!(!ds.same_set(&4, &0).unwrap());

        ds.union(&2, &4).unwrap();
        assert!(ds.same_set(&2, &4).unwrap());
        assert!(ds.same_set(&4, &2).unwrap());

        ds.union(&4, &2).unwrap();
        assert!(ds.same_set(&2, &4).unwrap());
        assert!(ds.same_set(&4, &2).unwrap());

        ds.union(&2, &6).unwrap();
        assert!(ds.same_set(&2, &6).unwrap());
        assert!(ds.same_set(&6, &4).unwrap());

        ds.union(&0, &7).unwrap();
        ds.union(&5, &0).unwrap();
        assert!(!ds.same_set(&5, &2).unwrap());
        assert!(ds.same_set(&6, &4).unwrap());

        ds.union(&5, &6).unwrap();
        ds.union(&1, &3).unwrap();
        assert!(ds.same_set(&7, &2).unwrap());
        assert!(ds.same_set(&1, &3).unwrap());
        assert!(!ds.same_set(&1, &7).unwrap());
        assert!(!ds.same_set(&3, &0).unwrap());
    }
    /*
    #[test]
    fn multiple_threads() {
        let num_threads = 100;
        let capacity = 1000;

        let ds = Arc::new(ConcurrentDisjointSet::with_capacity(capacity));
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = Vec::with_capacity(num_threads);

        for i in 0..num_threads {
            let ds = Arc::clone(&ds);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                for j in 0..capacity / num_threads {
                    ds.add(num_threads * j + i).unwrap();
                }
                barrier.wait();
                for j in 0..capacity / num_threads {
                    ds.union(
                        &(num_threads * j + i),
                        &(num_threads * j + ((i + 1) % num_threads)),
                    )
                    .unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        for i in 0..capacity {
            for j in 0..capacity {
                if i / num_threads == j / num_threads {
                    assert!(ds.same_set(&i, &j).unwrap());
                } else {
                    assert!(!ds.same_set(&i, &j).unwrap());
                }
            }
        }
    }
    */
}
