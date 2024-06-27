use std::collections::VecDeque;

#[derive(Default)]
#[allow(clippy::module_name_repetitions)]
pub struct LruList<T: Clone + Eq + PartialEq>(VecDeque<T>);

impl<T: Clone + Eq + PartialEq> LruList<T> {
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self(VecDeque::with_capacity(n))
    }

    pub fn remove_by(&mut self, f: impl FnMut(&T) -> bool) {
        self.0.retain(f);
    }

    pub fn remove(&mut self, item: &T) {
        self.remove_by(|x| x != item);
    }

    pub fn refresh(&mut self, item: T) {
        self.remove(&item);
        self.0.push_back(item);
    }

    pub fn get_least_recently_used(&mut self) -> Option<T> {
        let front = self.0.pop_front()?;
        self.0.push_back(front.clone());
        Some(front)
    }
}
