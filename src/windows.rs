pub trait GrowingWindowsExt<T> {
    fn growing_windows<'a>(&'a self) -> impl Iterator<Item = &'a [T]>
    where
        T: 'a;
}

impl<T> GrowingWindowsExt<T> for [T] {
    fn growing_windows<'a>(&'a self) -> impl Iterator<Item = &'a [T]>
    where
        T: 'a,
    {
        (1..=self.len()).flat_map(|size| self.windows(size))
    }
}

pub trait ShrinkingWindowsExt<T> {
    fn shrinking_windows<'a>(&'a self) -> impl Iterator<Item = &'a [T]>
    where
        T: 'a;
}

impl<T> ShrinkingWindowsExt<T> for [T] {
    fn shrinking_windows<'a>(&'a self) -> impl Iterator<Item = &'a [T]>
    where
        T: 'a,
    {
        (1..=self.len()).rev().flat_map(|size| self.windows(size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_growing_windows() {
        let a = [1, 2, 3, 4, 5];

        let mut windows = a.growing_windows();

        assert_eq!(&[1], windows.next().unwrap());
        assert_eq!(&[2], windows.next().unwrap());
        assert_eq!(&[3], windows.next().unwrap());
        assert_eq!(&[4], windows.next().unwrap());
        assert_eq!(&[5], windows.next().unwrap());

        assert_eq!(&[1, 2], windows.next().unwrap());
        assert_eq!(&[2, 3], windows.next().unwrap());
        assert_eq!(&[3, 4], windows.next().unwrap());
        assert_eq!(&[4, 5], windows.next().unwrap());

        assert_eq!(&[1, 2, 3], windows.next().unwrap());
        assert_eq!(&[2, 3, 4], windows.next().unwrap());
        assert_eq!(&[3, 4, 5], windows.next().unwrap());

        assert_eq!(&[1, 2, 3, 4], windows.next().unwrap());
        assert_eq!(&[2, 3, 4, 5], windows.next().unwrap());

        assert_eq!(&[1, 2, 3, 4, 5], windows.next().unwrap());
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_shrinking_windows() {
        let a = [1, 2, 3, 4, 5];

        let mut windows = a.shrinking_windows();

        assert_eq!(&[1, 2, 3, 4, 5], windows.next().unwrap());

        assert_eq!(&[1, 2, 3, 4], windows.next().unwrap());
        assert_eq!(&[2, 3, 4, 5], windows.next().unwrap());

        assert_eq!(&[1, 2, 3], windows.next().unwrap());
        assert_eq!(&[2, 3, 4], windows.next().unwrap());
        assert_eq!(&[3, 4, 5], windows.next().unwrap());

        assert_eq!(&[1, 2], windows.next().unwrap());
        assert_eq!(&[2, 3], windows.next().unwrap());
        assert_eq!(&[3, 4], windows.next().unwrap());
        assert_eq!(&[4, 5], windows.next().unwrap());

        assert_eq!(&[1], windows.next().unwrap());
        assert_eq!(&[2], windows.next().unwrap());
        assert_eq!(&[3], windows.next().unwrap());
        assert_eq!(&[4], windows.next().unwrap());
        assert_eq!(&[5], windows.next().unwrap());
    }
}
