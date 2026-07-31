# crucible-plugin

The plugin contract: the `Deployment`, `Driver`, and `Observer` role traits a
plugin implements, the schema descriptors it advertises, and the registry that
resolves a plugin by name. Also holds the first-party plugins (docker, http,
mariadb), which implement the same contract a third-party plugin would.
