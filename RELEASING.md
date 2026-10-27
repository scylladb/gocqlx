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

Generated notes exclude PRs labeled `omit-from-release-notes`. The stock
`actions/labeler` action uses `.github/labeler.yml` to apply the label only when
all reported changed filenames match known support paths: documented README
files, `RELEASING.md`, Go `*_test.go` files, dedicated testdata directories,
`.github/`, or the known development settings
`.gitignore`, `.golangci.yml`, and `renovate.json`.

Production Go files (including the importable `gocqlxtest` package), runnable
examples, embedded migration example CQL, templates, root and example module
manifests, `Makefile`, and unknown paths stay in the notes. Mixed PRs stay in the
notes. Authors, titles, and broad `chore` or `dependencies` labels never
determine exclusion. Classification reads the current and previous rename paths from GitHub. A rename
from a runtime or unknown path stays visible even when its destination is a
support path. No file contents are inspected.

Labeling runs on PR opening, synchronization, reopening, editing, and closing,
including merge events. With `sync-labels: true`, the action removes the omission
label when the filename rules stop matching. Runs for the same PR are serialized.
The workflow confirms the target is a pull request before checkout or dependency
setup. Manual dispatch must run from the repository default branch; an ordinary
issue or unconfirmed target receives no label writes. The workflow checks out
trusted event tooling, never PR head code, including for fork PRs.

A supplemental guard reads the same `.github/labeler.yml` as the stock action.
It checks the complete paginated file count, file metadata, head/base revisions
and previous rename paths before classification. After the action, it repeats
the snapshot and verifies the actual omission label, including its logical name
regardless of stored letter casing. A successful action run that skipped its PR
does not establish classification. Logs identify the verified head, base, count,
snapshot and expected omission state.

Failed, skipped or unverified classification attempts remove only the omission
label from confirmed PRs, including after checkout or dependency setup failures.
Cleanup is best effort: API or permission failures remain visible and can leave
stale labels. Before/after checks do not guarantee atomicity or race freedom.
Other labels are preserved.

Manual omission-marker add/remove events are filtered by the local Jira callers;
other Jira events are unchanged. The marker serves GitHub release-note filtering.
Existing Jira copies are not cleaned, and other sync paths can restore them.

Before releasing, backfill support-only PRs that merged before this workflow:

```sh
gh workflow run label-release-notes.yml --ref master -f pr_number=123
```

Before pushing a release tag, inspect every merged PR intended for that release
range, including ordinary recent merges and historical backfills. Compare its
current head/base and changed files with the classifier's final verified log and
actual omission label; production changes and production-origin renames must
remain visible. For a stale or missing result, run the backfill command on
`master`, wait for its final verification, and recheck the files and label. A
green run without that verification, a skipped PR, or a failed cleanup is not
sufficient. Recover an unverified omission by removing the marker on GitHub and
retrying classification after the underlying error is resolved. Recheck after
any later relevant change before tagging.

Publication remains asynchronous; no automated release/classification barrier
is included in this PR or its later review cycles. If manual verification is
missed, published notes may omit runtime changes or include support changes.
Reruns preserve already published notes, and no follow-up synchronization work is
scheduled. Manually applying the label is an override that reclassification can
remove.

Run the local automation checks with:

```sh
npm ci --ignore-scripts --prefix .github
node --test .github/scripts/*.test.cjs
```
