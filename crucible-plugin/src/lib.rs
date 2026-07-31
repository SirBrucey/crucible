//! The plugin contract: schema descriptors, the per-role plugin traits, and the
//! in-process registry of the first-party plugins.

pub mod builtin;
pub mod registry;
pub mod role;
pub mod schema;

pub use registry::Registry;
pub use role::{Deployment, Driver, Observer};
pub use schema::{
    AttrDecl, AttrSchema, ClauseDecl, ClauseShape, CmpOp, HeadPattern, OpSig, Param, ParamType,
    ValueType,
};
