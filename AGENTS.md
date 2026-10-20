# Project guidance

- Keep changes small and directed by the user. The user intends to build this
  project hands-on; do not implement additional components speculatively.
- Start in Python. The choice of Rust or C++ for a future port is deferred.
- Use the existing hgraph fabric implementation and RFC 0026 as design
  references, not as instructions to copy the entire implementation.
- Keep the standalone package independent of hgraph until integration is
  explicitly requested.
- Add behavioural tests as functionality is introduced. For scaffold changes,
  validate package building and installation.
- Develop changes on a focused branch and submit a pull request.
