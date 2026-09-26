---
"benni": patch
---

The package no longer ships source maps, which halves the install: 720 kB unpacked instead of 1.63 MB, 191 kB packed instead of 442 kB.

The maps were more than half the tarball and bought nothing a user could reach. The build is unbundled and unminified, one `.mjs` per source file with the same names, so a stack trace into `dist/` already reads like the source. The declaration maps pointed at `src/*.ts`, which was never published, so go-to-definition could not follow them.
