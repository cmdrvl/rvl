/// Row-order alignment & row count validation (bd-1xy).
///
/// Row IDs are 1-based data record indices (blank records skipped by caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowPair<T> {
    pub row_id: usize,
    pub old: T,
    pub new: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowOrderError {
    pub rows_old: usize,
    pub rows_new: usize,
}

pub struct RowOrderAligner<I, J> {
    old: I,
    new: J,
    row_id: usize,
    rows_old: usize,
    rows_new: usize,
    done: bool,
}

impl<I, J> RowOrderAligner<I, J> {
    pub fn new(old: I, new: J) -> Self {
        Self {
            old,
            new,
            row_id: 0,
            rows_old: 0,
            rows_new: 0,
            done: false,
        }
    }
}

impl<I, J, T> Iterator for RowOrderAligner<I, J>
where
    I: Iterator<Item = T>,
    J: Iterator<Item = T>,
{
    type Item = Result<RowPair<T>, RowOrderError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let old_next = self.old.next();
        let new_next = self.new.next();
        match (old_next, new_next) {
            (Some(old), Some(new)) => {
                self.row_id += 1;
                self.rows_old += 1;
                self.rows_new += 1;
                Some(Ok(RowPair {
                    row_id: self.row_id,
                    old,
                    new,
                }))
            }
            (None, None) => {
                self.done = true;
                None
            }
            (Some(_old), None) => {
                self.rows_old += 1;
                // Consume remaining old rows to compute final counts.
                self.rows_old += self.old.by_ref().count();
                self.done = true;
                Some(Err(RowOrderError {
                    rows_old: self.rows_old,
                    rows_new: self.rows_new,
                }))
            }
            (None, Some(_new)) => {
                self.rows_new += 1;
                // Consume remaining new rows to compute final counts.
                self.rows_new += self.new.by_ref().count();
                self.done = true;
                Some(Err(RowOrderError {
                    rows_old: self.rows_old,
                    rows_new: self.rows_new,
                }))
            }
        }
    }
}

pub fn row_order_aligner<I, J, T>(old: I, new: J) -> RowOrderAligner<I::IntoIter, J::IntoIter>
where
    I: IntoIterator<Item = T>,
    J: IntoIterator<Item = T>,
{
    RowOrderAligner::new(old.into_iter(), new.into_iter())
}

#[cfg(test)]
mod tests {
    use super::{RowOrderError, row_order_aligner};

    #[test]
    fn aligner_pairs_rows_with_1_based_row_ids() {
        let old = vec![10, 20];
        let new = vec![11, 22];
        let mut aligner = row_order_aligner(old, new);

        let first = aligner.next().unwrap().unwrap();
        assert_eq!(first.row_id, 1);
        assert_eq!(first.old, 10);
        assert_eq!(first.new, 11);

        let second = aligner.next().unwrap().unwrap();
        assert_eq!(second.row_id, 2);
        assert_eq!(second.old, 20);
        assert_eq!(second.new, 22);

        assert!(aligner.next().is_none());
    }

    #[test]
    fn aligner_detects_rowcount_mismatch_old_longer() {
        let old = vec![1, 2, 3];
        let new = vec![9, 8];
        let mut aligner = row_order_aligner(old, new);

        assert!(aligner.next().unwrap().is_ok());
        assert!(aligner.next().unwrap().is_ok());

        let err = aligner.next().unwrap().unwrap_err();
        assert_eq!(
            err,
            RowOrderError {
                rows_old: 3,
                rows_new: 2
            }
        );
        assert!(aligner.next().is_none());
    }

    #[test]
    fn aligner_detects_rowcount_mismatch_new_longer() {
        let old = vec![1];
        let new = vec![9, 8, 7];
        let mut aligner = row_order_aligner(old, new);

        assert!(aligner.next().unwrap().is_ok());

        let err = aligner.next().unwrap().unwrap_err();
        assert_eq!(
            err,
            RowOrderError {
                rows_old: 1,
                rows_new: 3
            }
        );
        assert!(aligner.next().is_none());
    }
}
