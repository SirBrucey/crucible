//! The `MariaDB` observer plugin.

use crucible_core::schema::{
    AttrDecl, AttrSchema, ClauseDecl, ClauseShape, CmpOp, HeadPattern, OpSig, ValueType,
};

use crate::role::Observer;

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

    /// Which database holds the state is discovered rather than declared, since
    /// the deployment is what creates it. Only the credentials to read it with
    /// are the author's to give, and they default to the unprivileged case.
    fn attr_schema() -> AttrSchema {
        AttrSchema::new(vec![
            AttrDecl::optional("user", ValueType::Str),
            AttrDecl::optional("password", ValueType::Str),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::Mariadb;
    use crate::role::Observer;
    use crucible_core::schema::{ClauseShape, CmpOp, HeadPattern, ValueType};

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
