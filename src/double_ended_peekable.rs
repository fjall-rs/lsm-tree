//! A fork of <https://github.com/dodomorandi/double-ended-peekable>
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
        if matches!(self, Self::Unpeeked) {
            *self = Self::Peeked(f());
        }

        let Self::Peeked(peeked) = self else {
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
            Self::Unpeeked | Self::Peeked(None) => None,
            Self::Peeked(Some(peeked)) => Some(peeked),
        }
    }

    fn take(&mut self) -> Self {
        mem::replace(self, Self::Unpeeked)
    }

    fn into_peeked_value(self) -> Option<T> {
        match self {
            Self::Unpeeked | Self::Peeked(None) => None,
            Self::Peeked(Some(peeked)) => Some(peeked),
        }
    }
}
