//! The Docker deployment plugin.

use crucible_core::{
    plan,
    schema::{AttrDecl, AttrSchema, ValueType},
};

use crate::role::Deployment;

/// Brings services up as Docker containers.
pub struct Docker;

/// What Docker needs to run one service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceConfig {
    pub name: String,
    pub image: String,
    pub port: u16,
    /// Environment variables, one per entry in `KEY=value` form.
    pub env: Vec<String>,
    /// Command to run inside the container as a healthcheck; empty means the
    /// image's own healthcheck.
    pub healthcheck: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("service `{service}` has no usable `{attr}`")]
    Attr { service: String, attr: &'static str },
    #[error("service `{service}`: {port} is not a port number")]
    Port { service: String, port: i64 },
}

impl Deployment for Docker {
    const NAME: &'static str = "docker";
    type Config = ServiceConfig;
    type Error = Error;

    fn attr_schema() -> AttrSchema {
        AttrSchema::new(vec![
            AttrDecl::required("image", ValueType::Str),
            AttrDecl::required("port", ValueType::Int),
            AttrDecl::optional("env", ValueType::List(Box::new(ValueType::Str))),
            AttrDecl::optional("healthcheck", ValueType::List(Box::new(ValueType::Str))),
        ])
    }

    fn bind(service: &plan::Service) -> Result<Self::Config, Self::Error> {
        let missing = |attr| Error::Attr {
            service: service.name.clone(),
            attr,
        };

        let image = service
            .attr("image")
            .and_then(plan::Value::as_str)
            .ok_or_else(|| missing("image"))?;
        let port = service
            .attr("port")
            .and_then(plan::Value::as_int)
            .ok_or_else(|| missing("port"))?;

        Ok(ServiceConfig {
            name: service.name.clone(),
            image: image.to_owned(),
            port: u16::try_from(port).map_err(|_| Error::Port {
                service: service.name.clone(),
                port,
            })?,
            env: owned_strs(service, "env")?,
            healthcheck: owned_strs(service, "healthcheck")?,
        })
    }
}

/// An optional list-of-strings attribute, empty when the service omits it.
fn owned_strs(service: &plan::Service, attr: &'static str) -> Result<Vec<String>, Error> {
    let Some(value) = service.attr(attr) else {
        return Ok(Vec::new());
    };
    let strs = value.as_strs().ok_or(Error::Attr {
        service: service.name.clone(),
        attr,
    })?;
    Ok(strs.into_iter().map(str::to_owned).collect())
}

#[cfg(test)]
mod tests {
    use super::{Docker, Error, ServiceConfig};
    use crate::role::Deployment;
    use crucible_core::{plan, schema::ValueType};

    fn service(attrs: Vec<(&str, plan::Value)>) -> plan::Service {
        plan::Service {
            name: "api".into(),
            kinds: vec!["http".into()],
            attrs: attrs
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        }
    }

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

    #[test]
    fn a_service_binds_to_a_container_config() {
        let bound = Docker::bind(&service(vec![
            ("image", plan::Value::Str("example/api:1".into())),
            ("port", plan::Value::Int(8080)),
            (
                "env",
                plan::Value::List(vec![plan::Value::Str("A=1".into())]),
            ),
        ]))
        .expect("binds");

        assert_eq!(
            bound,
            ServiceConfig {
                name: "api".into(),
                image: "example/api:1".into(),
                port: 8080,
                env: vec!["A=1".into()],
                healthcheck: Vec::new(),
            },
        );
    }

    #[test]
    fn a_port_outside_the_port_range_is_rejected() {
        let bound = Docker::bind(&service(vec![
            ("image", plan::Value::Str("example/api:1".into())),
            ("port", plan::Value::Int(70_000)),
        ]));
        assert!(matches!(bound, Err(Error::Port { .. })));
    }
}
