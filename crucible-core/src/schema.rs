//! Schema descriptors: the data a plugin advertises about the attributes,
//! operations, and observables it accepts.

/// The type of a value in a schema.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ValueType {
    Str,
    Int,
    Bool,
    Duration,
    /// One value, of whichever kind the fleet turns out to hold, ie: a field in a
    /// JSON body. What the scenario compares it against says which kind
    /// was meant.
    Scalar,
    List(Box<ValueType>),
    /// A map whose keys are names and whose values are all of one type.
    MapOf(Box<ValueType>),
    Map,
    ServiceRef,
}

/// One attribute a deployment plugin accepts inside a `service { ... }` body.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AttrDecl {
    pub name: String,
    pub ty: ValueType,
    pub required: bool,
}

impl AttrDecl {
    #[must_use]
    pub fn required(name: &str, ty: ValueType) -> Self {
        Self {
            name: name.to_owned(),
            ty,
            required: true,
        }
    }

    #[must_use]
    pub fn optional(name: &str, ty: ValueType) -> Self {
        Self {
            name: name.to_owned(),
            ty,
            required: false,
        }
    }
}

/// The attribute schema for a deployment plugin's service body.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AttrSchema {
    pub attrs: Vec<AttrDecl>,
}

impl AttrSchema {
    #[must_use]
    pub fn new(attrs: Vec<AttrDecl>) -> Self {
        Self { attrs }
    }

    /// The declaration for `name`, if the schema has one.
    #[must_use]
    pub fn attr(&self, name: &str) -> Option<&AttrDecl> {
        self.attrs.iter().find(|a| a.name == name)
    }
}

/// How an operation's head is matched against a `do` or `expect` head.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum HeadPattern {
    /// A fixed operation name, e.g. `POST`.
    Exact(String),
    /// One name per wildcard segment, then a fixed tail. The plugin parses for
    /// valid segments.
    Wildcard { segments: Vec<String>, tail: String },
}

impl HeadPattern {
    #[must_use]
    pub fn exact(name: &str) -> Self {
        Self::Exact(name.to_owned())
    }

    #[must_use]
    pub fn wildcard(segments: &[&str], tail: &str) -> Self {
        Self::Wildcard {
            segments: segments.iter().map(|s| (*s).to_owned()).collect(),
            tail: tail.to_owned(),
        }
    }
}

/// The type of a positional operation argument.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ParamType {
    ServiceRef,
    Str,
    Int,
    Path,
    Ident,
    Matcher,
}

/// One positional parameter of an operation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Param {
    pub name: String,
    pub ty: ParamType,
    pub required: bool,
}

impl Param {
    #[must_use]
    pub fn required(name: &str, ty: ParamType) -> Self {
        Self {
            name: name.to_owned(),
            ty,
            required: true,
        }
    }

    #[must_use]
    pub fn optional(name: &str, ty: ParamType) -> Self {
        Self {
            name: name.to_owned(),
            ty,
            required: false,
        }
    }
}

/// The shape of a keyword clause's payload.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ClauseShape {
    /// A single `<column> = <value>` filter, as in `where`.
    Filter,
    /// A `{ ... }` map payload, as in `body`.
    Block,
    /// One value of the type it names, written `<keyword>: <value>`.
    Value(ValueType),
}

/// An optional keyword clause an operation accepts, e.g. `where` or `body`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ClauseDecl {
    pub keyword: String,
    pub shape: ClauseShape,
}

impl ClauseDecl {
    #[must_use]
    pub fn new(keyword: &str, shape: ClauseShape) -> Self {
        Self {
            keyword: keyword.to_owned(),
            shape,
        }
    }
}

/// A comparison operator an observable's result can be tested with.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    /// Every comparison operator.
    pub const ALL: [CmpOp; 6] = [
        CmpOp::Eq,
        CmpOp::Ne,
        CmpOp::Lt,
        CmpOp::Le,
        CmpOp::Gt,
        CmpOp::Ge,
    ];
}

/// Spelled as a scenario writes it.
impl std::fmt::Display for CmpOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CmpOp::Eq => "==",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        })
    }
}

/// The signature of one operation: a driver action (a `do` step) or an observer
/// observable (an `expect` predicate's left side).
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct OpSig {
    pub head: HeadPattern,
    pub params: Vec<Param>,
    pub clauses: Vec<ClauseDecl>,
    /// What this produces: an observable's reading, or the outcome an action
    /// answers with. `None` when the plugin does not say.
    pub result: Option<ValueType>,
    /// The comparisons allowed on an observable's result; empty for an action.
    pub cmp_ops: Vec<CmpOp>,
}

impl OpSig {
    /// A driver action: positional `params`, answering with `outcome`, which a
    /// step may state to be held to.
    #[must_use]
    pub fn action(head: HeadPattern, params: Vec<Param>, outcome: ValueType) -> Self {
        Self {
            head,
            params,
            clauses: Vec::new(),
            result: Some(outcome),
            cmp_ops: Vec::new(),
        }
    }

    /// An observer observable: yields `result`, testable with `cmp_ops`.
    #[must_use]
    pub fn observable(head: HeadPattern, result: ValueType, cmp_ops: Vec<CmpOp>) -> Self {
        Self {
            head,
            params: Vec::new(),
            clauses: Vec::new(),
            result: Some(result),
            cmp_ops,
        }
    }

    #[must_use]
    pub fn with_clause(mut self, clause: ClauseDecl) -> Self {
        self.clauses.push(clause);
        self
    }

    #[must_use]
    pub fn with_param(mut self, param: Param) -> Self {
        self.params.push(param);
        self
    }
}
