//! The Docker deployment plugin.

use crate::{
    role::Deployment,
    schema::{AttrDecl, AttrSchema, ValueType},
};

/// Brings services up as Docker containers.
pub struct Docker;

impl Deployment for Docker {
    const NAME: &'static str = "docker";

    fn attr_schema() -> AttrSchema {
        AttrSchema::new(vec![
            AttrDecl::required("image", ValueType::Str),
            AttrDecl::required("port", ValueType::Int),
            AttrDecl::optional("env", ValueType::List(Box::new(ValueType::Str))),
            AttrDecl::optional("healthcheck", ValueType::List(Box::new(ValueType::Str))),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::Docker;
    use crate::{role::Deployment, schema::ValueType};

    #[test]
    fn image_and_port_are_required_env_is_optional() {
        let schema = Docker::attr_schema();

        let image = schema.attr("image").expect("image is declared");
        assert!(image.required);
        assert_eq!(image.ty, ValueType::Str);

        let port = schema.attr("port").expect("port is declared");
        assert!(port.required);
        assert_eq!(port.ty, ValueType::Int);

        assert!(!schema.attr("env").expect("env is declared").required);
    }
}
