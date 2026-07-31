//! The `MariaDB` observer plugin.

use crate::{
    role::Observer,
    schema::{ClauseDecl, ClauseShape, CmpOp, HeadPattern, OpSig, ValueType},
};

/// Reads persisted state from a `MariaDB` database.
pub struct Mariadb;

impl Observer for Mariadb {
    const NAME: &'static str = "mariadb";

    fn signatures() -> Vec<OpSig> {
        vec![
            OpSig::observable(
                HeadPattern::wildcard("table", "count"),
                ValueType::Int,
                CmpOp::ALL.to_vec(),
            )
            .with_clause(ClauseDecl::new("where", ClauseShape::Filter)),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::Mariadb;
    use crate::{
        role::Observer,
        schema::{ClauseShape, CmpOp, HeadPattern, ValueType},
    };

    #[test]
    fn count_is_an_int_observable_with_a_where_filter() {
        let signatures = Mariadb::signatures();
        let count = signatures
            .iter()
            .find(|sig| matches!(&sig.head, HeadPattern::Wildcard { tail, .. } if tail == "count"))
            .expect("mariadb has a `<table>.count` observable");

        assert_eq!(count.result, Some(ValueType::Int));
        assert!(count.cmp_ops.contains(&CmpOp::Eq));
        assert!(
            count
                .clauses
                .iter()
                .any(|clause| clause.keyword == "where" && clause.shape == ClauseShape::Filter),
        );
    }
}
