//! The HTTP driver plugin.

use crate::{
    role::Driver,
    schema::{ClauseDecl, ClauseShape, HeadPattern, OpSig, Param, ParamType},
};

/// Drives HTTP requests against a service.
pub struct Http;

impl Driver for Http {
    const NAME: &'static str = "http";

    fn signatures() -> Vec<OpSig> {
        vec![
            OpSig::action(HeadPattern::exact("POST"), request_params())
                .with_clause(ClauseDecl::new("body", ClauseShape::Block)),
            OpSig::action(HeadPattern::exact("GET"), request_params()),
            OpSig::action(HeadPattern::exact("DELETE"), request_params()),
        ]
    }
}

fn request_params() -> Vec<Param> {
    vec![
        Param::required("service", ParamType::ServiceRef),
        Param::required("path", ParamType::Path),
    ]
}

#[cfg(test)]
mod tests {
    use super::Http;
    use crate::{
        role::Driver,
        schema::{ClauseShape, HeadPattern},
    };

    #[test]
    fn exposes_post_get_and_delete() {
        let heads: Vec<String> = Http::signatures()
            .into_iter()
            .filter_map(|sig| match sig.head {
                HeadPattern::Exact(name) => Some(name),
                HeadPattern::Wildcard { .. } => None,
            })
            .collect();
        assert_eq!(heads, ["POST", "GET", "DELETE"]);
    }

    #[test]
    fn post_takes_a_body_clause() {
        let post = Http::signatures()
            .into_iter()
            .find(|sig| matches!(&sig.head, HeadPattern::Exact(name) if name == "POST"))
            .expect("http has a POST action");
        assert!(
            post.clauses
                .iter()
                .any(|clause| clause.keyword == "body" && clause.shape == ClauseShape::Block),
        );
    }
}
