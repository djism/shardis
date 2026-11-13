# 002: go.mod is the single source of truth for the Go version
- Context: CI and the laptop must build with the same Go version, or "works on my machine"
  bugs creep in (new language features, different race-detector behavior).
- Options considered:
  1. CI reads the version from `go.mod` (`actions/setup-go` with `go-version-file`).
  2. Hard-code the version in the CI YAML.
  3. Test against a matrix of several Go versions.
- Decision: Option 1, with `go 1.26.1` in go.mod.
- Why: One place to bump. A matrix (option 3) matters for libraries other people import;
  Shardis ships as a binary we build ourselves, so one version is enough.
- Tradeoffs we accept: Anyone building from source needs Go 1.26.1+ (the `go` command will
  auto-download the right toolchain if theirs is older).
- What real Redis does: Not applicable (C, built with make/gcc/clang). The analogous
  practice is pinning the compiler in the build image.
- At bigger scale I'd: Build in a pinned container image (hermetic builds, like Bazel at
  Google), so the compiler, libc and every tool are identical everywhere.
