# 001: Configure with the standard `flag` package plus SHARDIS_* env vars
- Context: The same binary must run on a laptop, in Docker Compose and in a Kubernetes
  StatefulSet. Laptops like flags; containers like env vars (set from a ConfigMap).
- Options considered:
  1. Standard library `flag` + a ~20-line helper that reads `SHARDIS_<NAME>` as the default.
  2. `spf13/pflag` (+ `viper`): GNU-style flags, config files, env binding, many features.
  3. Env vars only: simplest for containers, clumsy for local experiments.
- Decision: Option 1. Precedence is flag > env var > built-in default.
- Why: Zero dependencies, and the helper is small enough to read in a minute. Making `Load`
  take a `getenv` function means tests use a fake environment, never the real one, so they
  can't leak into each other. A malformed env var (e.g. `SHARDIS_PORT=abc`) is a hard error
  instead of being ignored, because a typo in a k8s manifest should fail at startup, not
  silently run on the wrong port.
- Tradeoffs we accept: No config files, no `-p` short flags, and every new setting needs one
  line in `Load`. Port is capped at 55535 so the cluster bus port (port + 10000) still fits.
- What real Redis does: Uses a `redis.conf` file, with any option overridable on the command
  line (`redis-server --port 7000`). No env-var support built in; container images wrap it
  with scripts. We differ on purpose: env vars are the natural fit for Kubernetes.
- At bigger scale I'd: Add a config file (or a central config service), validate the whole
  config in one place with clear errors, and support reloading safe settings at runtime
  (like Redis's `CONFIG SET`) so changes don't require a restart that triggers a failover.

**Update (Iteration 1):** The default `--host` changed from `0.0.0.0` to `127.0.0.1`. There's no
authentication, so listening on every network interface must be an explicit choice. This
matches Redis's default `bind 127.0.0.1`. Docker/Kubernetes set `SHARDIS_HOST=0.0.0.0`. It also
sidesteps macOS's AirPlay Receiver, which listens on `*:7000`: binding the more specific
`127.0.0.1:7000` still works, while `0.0.0.0:7000` fails with "address already in use".
