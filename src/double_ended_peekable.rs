//! A fork of https://github.com/dodomorandi/double-ended-peekable
//! to allow accessing the inner type
//!
//! Also changes the generics a bit so it plays well with `self_cell`.

use core::{fmt::Debug, hash::Hash, hint::unreachable_unchecked, mem};

/// An _extension trait_ to create [`DoubleEndedPeekable`].
///
/// This has a blanket implementation for all types that implement [`Iterator`].
pub trait DoubleEndedPeekableExt<T, I: Iterator<Item = T>> {
    /// Creates an iterator which works similarly to [`Peekable`], but also provides additional
    /// functions if the underlying type implements [`DoubleEndedIterator`].
    ///
    /// See [`DoubleEndedPeekable`] for more information.
    ///
    /// [`Peekable`]: core::iter::Peekable
    fn double_ended_peekable(self) -> DoubleEndedPeekable<T, I>;
}

impl<T, I> DoubleEndedPeekableExt<T, I> for I
where
    I: Iterator<Item = T>,
{
    #[inline]
    fn double_ended_peekable(self) -> DoubleEndedPeekable<T, I> {
        DoubleEndedPeekable {
            iter: self,
            front: MaybePeeked::Unpeeked,
            back: MaybePeeked::Unpeeked,
        }
    }
}

/// An advanced version of [`Peekable`] that works well with double-ended iterators.
///
/// This `struct` is created by the [`double_ended_peekable`] method on [`DoubleEndedPeekableExt`].
///
/// [`Peekable`]: core::iter::Peekable
/// [`double_ended_peekable`]: DoubleEndedPeekableExt::double_ended_peekable
pub struct DoubleEndedPeekable<T, I: Iterator<Item = T>> {
    iter: I,
    front: MaybePeeked<T>,
    back: MaybePeeked<T>,
}

impl<T, I> DoubleEndedPeekable<T, I>
where
    I: Iterator<Item = T>,
{
    pub fn inner(&self) -> &I {
        &self.iter
    }

    pub fn inner_mut(&mut self) -> &mut I {
        &mut self.iter
    }

    /// Returns a reference to the `next()` value without advancing the iterator.
    ///
    /// See [`Peekable::peek`] for more information.
    ///
    /// [`Peekable::peek`]: core::iter::Peekable::peek
    #[inline]
    pub fn peek(&mut self) -> Option<&I::Item> {
        self.front
            .get_peeked_or_insert_with(|| self.iter.next())
            .as_ref()
            .or_else(|| self.back.peeked_value_ref())
    }
}

impl<T, I> DoubleEndedPeekable<T, I>
where
    I: DoubleEndedIterator<Item = T>,
{
    /// Returns a reference to the `next_back()` value without advancing the _back_ of the iterator.
    ///
    /// Like [`next_back`], if there is a value, it is wrapped in a `Some(T)`.
    /// But if the iteration is over, `None` is returned.
    ///
    /// [`next_back`]: DoubleEndedIterator::next_back
    ///
    /// Because `peek_back()` returns a reference, and many iterators iterate over references,
    /// there can be a possibly confusing situation where the return value is a double reference.
    /// You can see this effect in the examples below.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use double_ended_peekable::DoubleEndedPeekableExt;
    ///
    /// let xs = [1, 2, 3];
    ///
    /// let mut iter = xs.into_iter().double_ended_peekable();
    ///
    /// // peek_back() lets us see into the past of the future
    /// assert_eq!(iter.peek_back(), Some(&3));
    /// assert_eq!(iter.next_back(), Some(3));
    ///
    /// assert_eq!(iter.next_back(), Some(2));
    ///
    /// // The iterator does not advance even if we `peek_back` multiple times
    /// assert_eq!(iter.peek_back(), Some(&1));
    /// assert_eq!(iter.peek_back(), Some(&1));
    ///
    /// assert_eq!(iter.next_back(), Some(1));
    ///
    /// // After the iterator is finished, so is `peek_back()`
    /// assert_eq!(iter.peek_back(), None);
    /// assert_eq!(iter.next_back(), None);
    /// ```
    #[inline]
    pub fn peek_back(&mut self) -> Option<&I::Item> {
        self.back
            .get_peeked_or_insert_with(|| self.iter.next_back())
            .as_ref()
            .or_else(|| self.front.peeked_value_ref())
    }
}

impl<T, I> Iterator for DoubleEndedPeekable<T, I>
where
    I: Iterator<Item = T>,
{
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.front.take() {
            MaybePeeked::Peeked(out @ Some(_)) => out,
            MaybePeeked::Peeked(None) => self.back.take().into_peeked_value(),
            MaybePeeked::Unpeeked => match self.iter.next() {
                item @ Some(_) => item,
                None => self.back.take().into_peeked_value(),
            },
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.iter.size_hint();
        let additional = match (&self.front, &self.back) {
            (MaybePeeked::Peeked(_), MaybePeeked::Peeked(_)) => 2,
            (MaybePeeked::Peeked(_), _) | (_, MaybePeeked::Peeked(_)) => 1,
            (MaybePeeked::Unpeeked, MaybePeeked::Unpeeked) => 0,
        };

        (lower + additional, upper.map(|upper| upper + additional))
    }
}

impl<T, I> DoubleEndedIterator for DoubleEndedPeekable<T, I>
where
    I: DoubleEndedIterator<Item = T>,
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.back.take() {
            MaybePeeked::Peeked(out @ Some(_)) => out,
            MaybePeeked::Peeked(None) => self.front.take().into_peeked_value(),
            MaybePeeked::Unpeeked => match self.iter.next_back() {
                out @ Some(_) => out,
                None => self.front.take().into_peeked_value(),
            },
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum MaybePeeked<T> {
    #[default]
    Unpeeked,
    Peeked(Option<T>),
}

impl<T> MaybePeeked<T> {
    fn get_peeked_or_insert_with<F>(&mut self, f: F) -> &mut Option<T>
    where
        F: FnOnce() -> Option<T>,
    {
        if let MaybePeeked::Unpeeked = self {
            *self = MaybePeeked::Peeked(f());
        }

        let MaybePeeked::Peeked(peeked) = self else {
            // SAFETY: it cannot be `Unpeeked` because that case has been just replaced with
            // `Peeked`, and we only have two possible states.
            #[allow(unsafe_code)]
            unsafe {
                unreachable_unchecked()
            }
        };
        peeked
    }

    const fn peeked_value_ref(&self) -> Option<&T> {
        match self {
            MaybePeeked::Unpeeked | MaybePeeked::Peeked(None) => None,
            MaybePeeked::Peeked(Some(peeked)) => Some(peeked),
        }
    }

    fn peeked_value_mut(&mut self) -> Option<&mut T> {
        match self {
            MaybePeeked::Unpeeked | MaybePeeked::Peeked(None) => None,
            MaybePeeked::Peeked(Some(peeked)) => Some(peeked),
        }
    }

    const fn is_unpeeked(&self) -> bool {
        matches!(self, MaybePeeked::Unpeeked)
    }

    fn take(&mut self) -> Self {
        mem::replace(self, MaybePeeked::Unpeeked)
    }

    fn into_peeked_value(self) -> Option<T> {
        match self {
            MaybePeeked::Unpeeked | MaybePeeked::Peeked(None) => None,
            MaybePeeked::Peeked(Some(peeked)) => Some(peeked),
        }
    }
}
