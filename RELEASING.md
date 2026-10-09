# npm Release

This package publishes only from `.github/workflows/release.yml`; do not run `npm publish` from a workstation.

## One-time npmjs setup

The npm package must be public and the GitHub repository must be public for npm provenance. In npmjs package settings, add a **GitHub Actions Trusted Publisher** with these exact values:

- GitHub owner: `redkern`
- Repository: `node-red-gateway`
- Workflow filename: `release.yml`
- Environment: `npm`
- Allow direct `npm publish` for this publisher

Do not create or store an `NPM_TOKEN`. Configure the publisher on the package and complete the first successful publish within npm's publisher-configuration validation window. In GitHub, protect the `npm` environment with required reviewers and protect `v*` tags so only release maintainers can create them.

## Release order

1. Publish and verify `@redkern/node-red-kit@1.0.1` first; Gateway's lockfile resolves that registry artifact.
2. In the Gateway GitHub repository, confirm `package.json`, `package-lock.json`, and `CHANGELOG.md` have the same stable version and that the release SLO values match the approved deployment target.
3. Push the reviewed commit and wait for CI's full Node.js 22/24 × Node-RED 4.1/5 matrix.
4. Create and push the matching tag, for example `v1.0.0`. The release workflow repeats the matrix, load SLO, dependency audit, requirement and pack gates before OIDC publication.
5. Verify the registry version, tarball and provenance after workflow completion. With npm 12, `npm audit signatures @redkern/node-red-gateway` can verify published attestations.

The workflow intentionally rejects prerelease versions, tag mismatches, missing dated changelog entries, a non-public package/repository identity, or absent load thresholds.