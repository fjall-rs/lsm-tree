use crate::InternalValue;
use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};

// TODO: refactor error handling because it's horrible

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>> + 'a>;

/// Merges multiple KV iterators
pub struct Merger<'a> {
    iterators: Vec<DoubleEndedPeekable<BoxedIterator<'a>>>,
}

impl<'a> Merger<'a> {
    pub fn new(iterators: Vec<BoxedIterator<'a>>) -> Self {
        let iterators = iterators
            .into_iter()
            .map(DoubleEndedPeekableExt::double_ended_peekable)
            .collect::<Vec<_>>();

        Self { iterators }
    }

    pub fn peek(&mut self) -> Option<crate::Result<(usize, &InternalValue)>> {
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

        min.map(Ok)
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

impl<'a> Iterator for Merger<'a> {
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

impl<'a> DoubleEndedIterator for Merger<'a> {
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
