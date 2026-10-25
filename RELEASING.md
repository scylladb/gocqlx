# GitHub releases

After the usual review and validation, maintainers push a Go semantic version
tag such as `v3.0.4` to publish a GitHub release with generated notes. The tag
must resolve to the triggering commit and its major must match the `go.mod`
module path (`github.com/scylladb/gocqlx/v3` requires `v3.x.y`). Prerelease tags
such as `v3.1.0-rc.1` produce prereleases. The workflow creates no tags, bumps no
versions, and performs no package publishing.

Release runs are serialized per tag. Reruns preserve published releases and
curated notes. An existing draft fails visibly so a maintainer must publish or
remove it before retrying. Tag verification and module-path checks also run on
reruns.

## Keeping generated notes useful

Generated notes exclude PRs labeled `omit-from-release-notes`. A trusted
classifier applies the label only if every changed path is an explicitly known
support file: documented README files, `RELEASING.md`, Go `*_test.go` files,
dedicated testdata directories, `.github/`, or the known development settings
`.gitignore`, `.golangci.yml`, and `renovate.json`.

Production Go files (including the importable `gocqlxtest` package), runnable
examples, embedded migration example CQL, templates, root and example module
manifests, `Makefile`, and unknown paths stay in the notes. Mixed PRs stay in the
notes. Renames must have support paths on both sides. Authors, titles, and broad
`chore` or `dependencies` labels never determine exclusion.

Classification runs on PR opening, synchronization, reopening, editing, and
closing. It checks a complete paginated file list against stable head/base SHAs
and file counts, removes stale omission labels for mixed changes, and attempts
to remove the label before surfacing API or snapshot failures. Runs for the same
PR are serialized. It executes only default-branch classifier code, including
for fork PRs; PR code never runs with label-write permissions.

Before releasing, backfill support-only PRs that merged before this workflow:

```sh
gh workflow run label-release-notes.yml --ref master -f pr_number=123
```

Repeat for each relevant PR and verify the workflow succeeds before pushing the
release tag. Manually applying the label is a maintainer override; reclassifying
a PR can remove it. Existing published release notes are never rewritten.

Run the local automation checks with:

```sh
node --test .github/scripts/*.test.cjs
```
