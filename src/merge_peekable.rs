use crate::{BoxedIterator, InternalValue, UserKey};
use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};

// TODO: refactor error handling because it's horrible

pub struct MergePeekable<'a> {
    iterators: Vec<DoubleEndedPeekable<BoxedIterator<'a>>>,
}

impl<'a> MergePeekable<'a> {
    pub fn new(iterators: Vec<BoxedIterator<'a>>) -> Self {
        let iterators = iterators
            .into_iter()
            .map(DoubleEndedPeekableExt::double_ended_peekable)
            .collect::<Vec<_>>();

        Self { iterators }
    }

    /// Returns the highest version of the current user key.
    ///
    /// Only use during reverse iteration!!! Forward iteration is implicitly sorted already.
    pub fn get_highest_version(
        &mut self,
        current: &InternalValue,
    ) -> crate::Result<Option<InternalValue>> {
        let mut higher_value = None;

        while let Some(head_result) = self.peek_back() {
            match head_result {
                Ok((_, next)) => {
                    if next.key.user_key == current.key.user_key {
                        let next = self.next_back().expect("should exist");

                        let next = match next {
                            Ok(v) => v,
                            Err(e) => {
                                return Err(e);
                            }
                        };

                        // Keep popping off heap until we reach the next key
                        // Because the seqno's are stored in descending order
                        // The next item will definitely have a higher seqno, so
                        // we can just take it
                        higher_value = Some(next);
                    } else {
                        // Reached next user key now
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(higher_value)
    }

    /// Drains the lower bound until the next user key is reached.
    pub fn drain_key_min(&mut self, key: &UserKey) -> crate::Result<()> {
        for iter in &mut self.iterators {
            'inner: loop {
                if let Some(item) = iter.peek() {
                    if let Ok(item) = item {
                        if &item.key.user_key == key {
                            // Consume key
                            iter.next().expect("should not be empty")?;
                        } else {
                            // Reached next key, go to next iterator
                            break 'inner;
                        }
                    } else {
                        iter.next().expect("should not be empty")?;

                        panic!("logic error");
                    }
                } else {
                    // Iterator is empty, go to next
                    break 'inner;
                }
            }
        }

        Ok(())
    }

    pub fn peek_back(&mut self) -> Option<crate::Result<(usize, &InternalValue)>> {
        let mut idx_with_err = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                if val.is_err() {
                    idx_with_err = Some(idx);
                }
            }
        }

        if let Some(idx) = idx_with_err {
            let err = self
                .iterators
                .get_mut(idx)
                .expect("should exist")
                .next_back()
                .expect("should not be empty");

            if let Err(e) = err {
                return Some(Err(e));
            }

            panic!("logic error");
        }

        let mut max: Option<(usize, &InternalValue)> = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, max_val)) = max {
                            if val.key > max_val.key {
                                max = Some((idx, val));
                            }
                        } else {
                            max = Some((idx, val));
                        }
                    }
                    _ => panic!("already checked for errors"),
                }
            }
        }

        max.map(Ok)
    }
}

impl<'a> Iterator for MergePeekable<'a> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut idx_with_err = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek()).enumerate() {
            if let Some(val) = val {
                if val.is_err() {
                    idx_with_err = Some(idx);
                }
            }
        }

        if let Some(idx) = idx_with_err {
            let err = self
                .iterators
                .get_mut(idx)
                .expect("should exist")
                .next()
                .expect("should not be empty");

            if let Err(e) = err {
                return Some(Err(e));
            }

            panic!("logic error");
        }

        let mut min: Option<(usize, &InternalValue)> = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, min_val)) = min {
                            if val.key < min_val.key {
                                min = Some((idx, val));
                            }
                        } else {
                            min = Some((idx, val));
                        }
                    }
                    _ => panic!("already checked for errors"),
                }
            }
        }

        if let Some((idx, _)) = min {
            let value = self
                .iterators
                .get_mut(idx)?
                .next()?
                .expect("should not be error");

            Some(Ok(value))
        } else {
            None
        }
    }
}

impl<'a> DoubleEndedIterator for MergePeekable<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut idx_with_err = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                if val.is_err() {
                    idx_with_err = Some(idx);
                }
            }
        }

        if let Some(idx) = idx_with_err {
            let err = self
                .iterators
                .get_mut(idx)
                .expect("should exist")
                .next_back()
                .expect("should not be empty");

            if let Err(e) = err {
                return Some(Err(e));
            }

            panic!("logic error");
        }

        let mut max: Option<(usize, &InternalValue)> = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek_back()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, max_val)) = max {
                            if val.key > max_val.key {
                                max = Some((idx, val));
                            }
                        } else {
                            max = Some((idx, val));
                        }
                    }
                    _ => panic!("already checked for errors"),
                }
            }
        }

        if let Some((idx, _)) = max {
            let value = self
                .iterators
                .get_mut(idx)?
                .next_back()?
                .expect("should not be error");

            Some(Ok(value))
        } else {
            None
        }
    }
}
