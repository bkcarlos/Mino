# configs

Runtime configuration samples (logging, bus, recording, transport).
See the detailed design document section 20 for the configuration schema.

Configuration files use TOML. `mino.toml` contains the current logging schema;
application code logs through `//mino/common/logging:logging`, while
`InitializeLogging()` installs the default spdlog backend from the parsed TOML
configuration. The backend can be replaced by installing another `Logger`
implementation without changing call sites.
