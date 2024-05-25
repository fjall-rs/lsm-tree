use crate::{SeqNo, UserKey, UserValue};
use std::ops::RangeBounds;

/// Generic Tree API
#[allow(clippy::module_name_repetitions)]
pub trait AbstractTree {
    /// Scans the entire tree, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire tree: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the tree is empty, use [`Tree::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    ///
    /// assert_eq!(tree.len()?, 0);
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    /// assert_eq!(tree.len()?, 3);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn len(&self) -> crate::Result<usize> {
        let mut count = 0;

        // TODO: shouldn't write to block cache
        for item in self.iter() {
            let _ = item?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns `true` if the tree is empty.
    ///
    /// This operation has O(1) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// assert!(tree.is_empty()?);
    ///
    /// tree.insert("a", "abc", 0);
    /// assert!(!tree.is_empty()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn is_empty(&self) -> crate::Result<bool> {
        self.first_key_value().map(|x| x.is_none())
    }

    /// Returns the first key-value pair in the tree.
    /// The key in this pair is the minimum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    ///
    /// let (key, _) = tree.first_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn first_key_value(&self) -> crate::Result<Option<(UserKey, UserValue)>> {
        self.iter().next().transpose()
    }

    /// Returns the last key-value pair in the tree.
    /// The key in this pair is the maximum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    ///
    /// let (key, _) = tree.last_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn last_key_value(&self) -> crate::Result<Option<(UserKey, UserValue)>> {
        self.iter().next_back().transpose()
    }

    /// Returns an iterator that scans through the entire tree.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("f", "abc", 1);
    /// tree.insert("g", "abc", 2);
    /// assert_eq!(3, tree.iter().into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(clippy::iter_not_returning_iterator)]
    #[must_use]
    fn iter(&self) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + '_ {
        self.range::<UserKey, _>(..)
    }

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("f", "abc", 1);
    /// tree.insert("g", "abc", 2);
    /// assert_eq!(2, tree.range("a"..="f").into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + '_;

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("ab", "abc", 1);
    /// tree.insert("abc", "abc", 2);
    /// assert_eq!(2, tree.prefix("ab").count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + '_;

    /// Retrieves an item from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// tree.insert("a", "my_value", 0);
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserValue>>;

    /// Inserts a key-value pair into the tree.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// tree.insert("a", "abc", 0);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, seqno: SeqNo) -> (u32, u32);

    /// Removes an item from the tree.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// tree.insert("a", "abc", 0);
    ///
    /// let item = tree.get("a")?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// tree.remove("a", 1);
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn remove<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> (u32, u32);
}
