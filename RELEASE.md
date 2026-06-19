# Release Process

## Versioning

ibcore follows [Semantic Versioning](https://semver.org/).

- **MAJOR** (x.0.0): breaking API changes — removed methods, changed signatures, new required parameters
- **MINOR** (0.x.0): new features, new public API, non-breaking additions
- **PATCH** (0.0.x): bug fixes, documentation, internal refactors

While in `0.x` (pre-1.0), the API is considered unstable. Minor versions may contain small breaking changes; these will be documented prominently in the changelog.

## Release Steps

1. **Update version** in `Cargo.toml`
2. **Update CHANGELOG.md** — add new section, link to the coming tag
3. **Run tests:** `cargo test --lib` (80/80 must pass)
4. **Run clippy:** `cargo clippy -- -D warnings` (must be clean)
5. **Commit:** `git commit -am "release: vX.Y.Z"`
6. **Tag:** `git tag -a vX.Y.Z -m "ibcore vX.Y.Z"`
7. **Push:** `git push && git push --tags`
8. **Create GitHub Release:** use the changelog section as release notes
9. **Publish to crates.io:** `cargo publish` (if ready for public consumption)

## Crates.io Publishing

```bash
# One-time setup
cargo login

# Publish
cd ~/ibcore
cargo publish
```

Note: crates.io packages are immutable. Test thoroughly before publishing.
Prefer GitHub-only releases during early development (0.x).
