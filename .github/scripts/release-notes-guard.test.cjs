// Copyright (C) 2026 ScyllaDB
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {test} = require('node:test');
const yaml = require('js-yaml');
const guard = require('./release-notes-guard.cjs');

const repo = {owner: 'scylladb', repo: 'gocqlx'};
const label = 'omit-from-release-notes';
const file = (filename, extra = {}) => ({filename, status: 'modified', ...extra});
const workflowText = readFileSync(join(__dirname, '../workflows/label-release-notes.yml'), 'utf8');
const workflow = yaml.load(workflowText);
const steps = workflow.jobs.classify.steps;
const scripts = steps.filter(step => step.with?.script).map(step => step.with.script);
const collectScript = steps.find(step => step.id === 'guard').with.script;
const confirmScript = steps.find(step => step.id === 'target').with.script;
const verifyScript = steps.at(-1).with.script;
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

function fixture(options = {}) {
  const files = options.files ?? [file('README.md')];
  const labels = new Set(options.labels ?? ['unrelated']);
  const calls = [];
  let gets = 0;
  let listings = 0;
  const initial = {number: 20, head: {sha: 'a'.repeat(40)}, base: {sha: 'b'.repeat(40)}, changed_files: files.length};
  const github = {rest: {pulls: {
    get: async params => {
      assert.deepEqual(params, {...repo, pull_number: 20});
      calls.push('get');
      gets++;
      if (options.failGetAt === gets) throw new Error('PR lookup unavailable');
      return {data: {...initial, ...options.revisions?.[gets - 1]}};
    },
    listFiles: () => {}
  }, issues: {
    listLabelsOnIssue: () => {},
    removeLabel: async params => {
      assert.deepEqual(params, {...repo, issue_number: 20, name: label});
      calls.push('remove');
      if (options.cleanupFailure) throw new Error('Removal unavailable');
      // GitHub looks up the logical label without case sensitivity while
      // preserving its stored spelling in returned metadata.
      const actual = [...labels].find(name => name.toLowerCase() === label);
      if (actual === undefined) throw {status: 404};
      labels.delete(actual);
    }
  }}, paginate: async (method, params) => {
    assert.equal(params.per_page, 100);
    if (method === github.rest.pulls.listFiles) {
      assert.deepEqual(params, {...repo, pull_number: 20, per_page: 100});
      calls.push('files');
      if (options.fileFailure) throw new Error('File listing unavailable');
      return options.listings?.[listings++] ?? files;
    }
    assert.equal(method, github.rest.issues.listLabelsOnIssue);
    assert.deepEqual(params, {...repo, issue_number: 20, per_page: 100});
    calls.push('labels');
    if (options.labelFailure) throw new Error('Labels unavailable');
    return options.labelMetadata ?? [...labels].map(name => ({name}));
  }};
  return {github, labels, calls};
}

async function adapter(f, hash, overrides = {}, load = () => guard) {
  const env = {PR_NUMBER: '20', CONFIRMED_PR: 'true', TARGET_OUTCOME: 'success',
    CHECKOUT_OUTCOME: 'success', INSTALL_OUTCOME: 'success',
    GUARD_OUTCOME: 'success', LABELER_OUTCOME: 'success', UNSAFE_RENAME: 'false',
    SNAPSHOT_HASH: hash, ...overrides};
  const logs = [];
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', verifyScript);
  let error;
  try {
    await run(f.github, {repo}, {info: text => logs.push(text), error: text => logs.push(text)},
      load, {env});
  } catch (caught) { error = caught; }
  return {error, logs};
}

test('canonical policy matches mixed support files and keeps runtime or unknown paths visible', async () => {
  const files = ['.github/workflows/github-release.yml', '.github/release.yml', '.github/labeler.yml',
    '.github/.hidden', 'README.md', 'qb/README.md', 'migrate/README.md', 'examples/basic/README.md',
    'queryx_test.go', 'qb/select_test.go', 'testdata/schema.cql', 'migrate/testdata/001_init.cql',
    'cmd/schemagen/testdata/models.go', '.gitignore', '.golangci.yml', 'renovate.json',
    'RELEASING.md'].map(name => file(name));
  assert.equal((await guard.snapshot(fixture({files}).github, repo, 20)).omit, true);
  for (const name of ['queryx.go', 'gocqlxtest/session.go', 'go.mod', 'go.sum', 'Makefile', 'LICENSE',
    'examples/basic/main.go', 'examples/basic/go.mod', 'migrate/example/001_init.cql',
    'cmd/schemagen/tmpl.go', 'unknown.txt']) {
    const current = await guard.snapshot(fixture({files: [...files, file(name)]}).github, repo, 20);
    assert.equal(current.omit, false, name);
  }
});

test('runnable examples stay visible alone, in mixed PRs, and through renames', async () => {
  for (const filename of ['example_test.go', 'migrate/example/example_test.go',
    'migrate/example/cql/example_test.go']) {
    for (const files of [[file(filename)], [file('README.md'), file(filename)]]) {
      assert.equal((await guard.snapshot(fixture({files}).github, repo, 20)).omit, false, filename);
    }
    const fromExample = await guard.snapshot(fixture({files: [file('README.md', {
      status: 'renamed', previous_filename: filename})]}).github, repo, 20);
    assert.equal(fromExample.omit, false, filename);
    assert.equal(fromExample.unsafeRename, true, filename);
    const toExample = await guard.snapshot(fixture({files: [file(filename, {
      status: 'renamed', previous_filename: 'README.md'})]}).github, repo, 20);
    assert.equal(toExample.omit, false, filename);
  }
  for (const filename of ['queryx_test.go', 'migrate/migrate_test.go', 'qb/select_test.go']) {
    assert.equal((await guard.snapshot(fixture({files: [file(filename)]}).github, repo, 20)).omit, true, filename);
  }
});

test('renames use the actual canonical policy for both prior and current paths', async () => {
  for (const [previous, omit] of [['README.md', true], ['queryx.go', false], ['unknown', false]]) {
    const current = await guard.snapshot(fixture({files: [file('RELEASING.md', {
      status: 'renamed', previous_filename: previous})]}).github, repo, 20);
    assert.equal(current.omit, omit);
    assert.equal(current.unsafeRename, !omit);
  }
});

test('support-to-runtime renames remain visible without skipping the stock action', async () => {
  const current = await guard.snapshot(fixture({files: [file('queryx.go', {
    status: 'renamed', previous_filename: 'queryx_test.go'})]}).github, repo, 20);
  assert.equal(current.omit, false);
  assert.equal(current.unsafeRename, false);
});

test('unsupported config shapes fail instead of approximating a second policy', () => {
  for (const source of ['{}', 'omit-from-release-notes: []',
    'omit-from-release-notes: [{head-branch: [main]}]',
    'omit-from-release-notes: [{changed-files: [{any-glob-to-any-file: ["**"]}]}]',
    'omit-from-release-notes: [{changed-files: [{all-globs-to-all-files: []}]}]',
    'other-label: [{changed-files: [{all-globs-to-all-files: ["**"]}]}]']) {
    assert.throws(() => guard.policy(source), /Unsupported labeler configuration/);
  }
});

test('PR input rejects malformed, ambiguous and unsafe numbers', () => {
  assert.equal(guard.parseNumber('20'), 20);
  for (const value of ['', '0', '-1', '01', '1.0', '1e2', ' 20', '20\n',
    '20,21', '${{ arbitrary }}', '9007199254740992', undefined, 20]) {
    assert.throws(() => guard.parseNumber(value), /canonical positive safe PR number/);
  }
});

test('pagination requests full metadata and accepts a complete listing beyond one page', async () => {
  const f = fixture({files: Array.from({length: 105}, (_, i) => file(`testdata/fixture-${i}.cql`))});
  const snapshot = await guard.snapshot(f.github, repo, 20);
  assert.equal(snapshot.count, 105);
  assert.equal(snapshot.omit, true);
  assert.match(snapshot.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(f.calls, ['get', 'files', 'get']);
});

test('zero, incomplete, truncated, duplicate and invalid file evidence fail visibly', async () => {
  const cases = [
    {files: []},
    {revisions: [{changed_files: 2}, {changed_files: 2}]},
    {files: [file('README.md'), file('README.md')]},
    {files: [file('README.md', {status: 'renamed'})]},
    {files: [file('README.md', {status: undefined})]},
    {files: [file('README.md', {previous_filename: ''})]},
    {files: [file('README.md')], listings: [[]]}
  ];
  for (const options of cases) await assert.rejects(guard.snapshot(fixture(options).github, repo, 20));
});

test('head, base and count changes during collection are rejected', async () => {
  for (const changed of [{head: {sha: 'c'.repeat(40)}}, {base: {sha: 'c'.repeat(40)}}, {changed_files: 2}]) {
    const f = fixture({revisions: [{}, changed]});
    await assert.rejects(guard.snapshot(f.github, repo, 20), /revisions changed/);
  }
});

test('verification detects before/after revisions and filename, status or prior-path changes', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [
    {revisions: [{head: {sha: 'c'.repeat(40)}}, {head: {sha: 'c'.repeat(40)}}]},
    {revisions: [{base: {sha: 'c'.repeat(40)}}, {base: {sha: 'c'.repeat(40)}}]},
    {files: [file('docs/new.md')]},
    {files: [file('README.md', {status: 'added'})]},
    {files: [file('README.md', {status: 'renamed', previous_filename: 'docs/old.md'})]}
  ]) {
    await assert.rejects(guard.verify(fixture(options).github, repo, 20, original.hash), /snapshot changed/);
  }
  const f = fixture({labels: [label], revisions: [{}, {}, {head: {sha: 'c'.repeat(40)}}]});
  await assert.rejects(guard.verify(f.github, repo, 20, original.hash), /revisions changed during label/);
});

test('lookup, listing and actual-label lookup failures propagate', async () => {
  await assert.rejects(guard.snapshot(fixture({failGetAt: 1}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({failGetAt: 2}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({fileFailure: true}).github, repo, 20), /listing unavailable/);
  const original = await guard.snapshot(fixture().github, repo, 20);
  await assert.rejects(guard.verify(fixture({labelFailure: true}).github, repo, 20, original.hash), /Labels unavailable/);
});

test('actual-label verification catches stale state after internal action skip', async () => {
  for (const [files, labels] of [[[file('README.md')], []], [[file('queryx.go')], [label]]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Actual omission label/);
    assert.equal(f.labels.has(label), false);
    assert.equal(f.labels.has('unrelated'), true);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('failed final snapshot, lookup or label retrieval attempts cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [{files: [file('docs/changed.md')]}, {failGetAt: 1},
    {fileFailure: true}, {labelFailure: true}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const {error} = await adapter(f, original.hash);
    assert.ok(error);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('successful verification checks actual labels and preserves unrelated labels', async () => {
  for (const [files, labels] of [[[file('README.md')], [label]], [[file('queryx.go')], []]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error, logs} = await adapter(f, original.hash);
    assert.ifError(error);
    assert.match(logs[0], /Verified head=a{40} base=b{40} count=1 snapshot=.*actual label checked/);
    assert.equal(f.labels.has('unrelated'), true);
    assert.ok(!f.calls.includes('remove'));
  }
});

test('actual logical marker identity preserves support omissions and rejects runtime omissions regardless of casing', async () => {
  const marker = 'Omit-from-release-notes';
  const other = 'OMIT-FROM-RELEASE-NOTES-extra';
  const prefix = 'prefix-omit-from-release-notes';
  for (const [name, omit] of [['README.md', true], ['queryx.go', false]]) {
    const f = fixture({files: [file(name)], labels: [marker, other, prefix, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error} = await adapter(f, original.hash);
    if (omit) {
      assert.ifError(error);
      assert.equal(f.labels.has(marker), true);
      assert.ok(!f.calls.includes('remove'));
    } else {
      assert.match(error.message, /Actual omission label/);
      assert.equal(f.labels.has(marker), false);
      assert.equal(f.calls.filter(call => call === 'remove').length, 1);
    }
    assert.equal(f.labels.has(other), true);
    assert.equal(f.labels.has(prefix), true);
    assert.equal(f.labels.has('unrelated'), true);
  }
});

test('invalid actual-label metadata fails verification and attempts canonical cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const item of [null, {}, {name: 42}, {name: ''}]) {
    const f = fixture({labels: [label, 'unrelated'], labelMetadata: [item]});
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Invalid label metadata/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('intentional unsafe-rename skip removes omission label and verifies visible state', async () => {
  const f = fixture({files: [file('README.md', {status: 'renamed', previous_filename: 'queryx.go'})],
    labels: [label, 'unrelated']});
  const original = await guard.snapshot(f.github, repo, 20);
  const {error, logs} = await adapter(f, original.hash, {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.ifError(error);
  assert.match(logs[0], /omit=false; actual label checked/);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.equal(f.calls.filter(call => call === 'remove').length, 1);
});

test('failed or skipped setup, guard and labeler clean up without requiring the helper', async () => {
  for (const key of ['TARGET_OUTCOME', 'CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    for (const outcome of ['failure', 'skipped', 'cancelled', '']) {
      const f = fixture({labels: [label, 'unrelated']});
      const {error} = await adapter(f, '', {[key]: outcome}, () => { throw new Error('Must not require helper'); });
      assert.match(error.message, /predecessor failed or unexpectedly skipped/);
      assert.deepEqual([...f.labels], ['unrelated']);
      assert.deepEqual(f.calls, ['remove']);
    }
  }
  const f = fixture({labels: [label]});
  const {error} = await adapter(f, '', {}, () => { throw new Error('Helper missing'); });
  assert.match(error.message, /Helper missing/);
  assert.deepEqual(f.calls, ['remove']);
});

test('cleanup API failures remain visible without losing the original failure or retrying', async () => {
  const f = fixture({cleanupFailure: true});
  const result = await adapter(f, '', {INSTALL_OUTCOME: 'failure'});
  assert.match(result.error.message, /predecessor failed/);
  assert.match(result.logs[0], /cleanup failed: Removal unavailable/);
  assert.deepEqual(f.calls, ['remove']);
  const unsafe = fixture({cleanupFailure: true});
  const result2 = await adapter(unsafe, '', {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.match(result2.error.message, /Removal unavailable/);
  assert.deepEqual(unsafe.calls, ['remove']);
});

async function confirm(f, context = {repo, eventName: 'workflow_dispatch'}) {
  const outputs = {};
  const run = new AsyncFunction('github', 'context', 'core', 'process', confirmScript);
  let error;
  try {
    await run(f.github, context, {setOutput: (key, value) => {outputs[key] = value;}},
      {env: {PR_NUMBER: '20'}});
  } catch (caught) {error = caught;}
  return {outputs, error};
}

test('target confirmation precedes checkout and accepts manual PR or trusted PR event identity', async () => {
  assert.ok(steps.findIndex(step => step.id === 'target') < steps.findIndex(step => step.id === 'checkout'));
  const manual = fixture();
  const first = await confirm(manual);
  assert.ifError(first.error);
  assert.deepEqual(first.outputs, {'confirmed-pr': 'true'});
  assert.deepEqual(manual.calls, ['get']);
  const event = fixture({failGetAt: 1});
  const second = await confirm(event, {repo, eventName: 'pull_request_target',
    payload: {pull_request: {number: 20}}});
  assert.ifError(second.error);
  assert.deepEqual(second.outputs, {'confirmed-pr': 'true'});
  assert.deepEqual(event.calls, []);
});

test('ordinary issue, unavailable lookup and mismatched identity never enable label mutation', async () => {
  for (const options of [{failGetAt: 1}, {revisions: [{number: 21}]}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const target = await confirm(f);
    assert.ok(target.error);
    assert.deepEqual(target.outputs, {});
    const result = await adapter(f, '', {CONFIRMED_PR: '', TARGET_OUTCOME: 'failure'},
      () => {throw new Error('Must not load helper for unconfirmed target');});
    assert.match(result.error.message, /not confirmed as a pull request/);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
    assert.ok(!f.calls.includes('remove'));
  }
  const issue = fixture({labels: [label, 'unrelated']});
  issue.github.rest.pulls.get = async () => {throw {status: 404};};
  const target = await confirm(issue);
  assert.equal(target.error.status, 404);
  const result = await adapter(issue, '', {CONFIRMED_PR: '', TARGET_OUTCOME: 'failure'});
  assert.match(result.error.message, /labels were not changed/);
  assert.deepEqual([...issue.labels], [label, 'unrelated']);
  assert.ok(!issue.calls.includes('remove'));
});

test('confirmed manual PR retains helper-independent cleanup after later failures', async () => {
  for (const key of ['CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    const f = fixture({labels: [label, 'unrelated']});
    const target = await confirm(f);
    assert.ifError(target.error);
    const result = await adapter(f, '', {CONFIRMED_PR: target.outputs['confirmed-pr'], [key]: 'failure'},
      () => {throw new Error('Must not need helper for predecessor failure');});
    assert.match(result.error.message, /predecessor failed/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.deepEqual(f.calls, ['get', 'remove']);
  }
});

test('workflow adapters use environment input and emit bounded verified snapshot outputs', async () => {
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', collectScript);
  const outputs = {};
  const f = fixture();
  await run(f.github, {repo}, {setOutput: (key, value) => { outputs[key] = value; }, info: () => {}},
    () => guard, {env: {PR_NUMBER: '20'}});
  assert.deepEqual(Object.keys(outputs), ['hash', 'unsafe-rename']);
  assert.equal(outputs['unsafe-rename'], 'false');
  const invalid = await adapter(f, '', {PR_NUMBER: '01'});
  assert.match(invalid.error.message, /canonical positive safe PR number/);
  assert.equal(steps.at(-1).if, 'always()');
  assert.equal(steps.find(step => step.id === 'checkout').with.ref, '${{ github.sha }}');
  assert.equal(steps.find(step => step.id === 'checkout').with['persist-credentials'], false);
  assert.equal(steps.find(step => step.id === 'install').run, 'npm ci --ignore-scripts --prefix .github');
  assert.equal(workflow.on.workflow_dispatch.inputs.pr_number.type, 'number');
  assert.equal(workflow.jobs.classify.env.PR_NUMBER, '${{ github.event.pull_request.number || inputs.pr_number }}');
  const action = steps.find(step => step.id === 'labeler');
  assert.equal(action.if, "success() && steps.guard.outputs.unsafe-rename == 'false'");
  assert.equal(action.with['sync-labels'], true);
  assert.equal(action.with['pr-number'], '${{ env.PR_NUMBER }}');
  for (const script of scripts) assert.ok(!script.includes('${{'));
});

test('merged-PR backfills load event tooling rather than the old PR base or head', () => {
  const expression = steps.find(step => step.id === 'checkout').with.ref.replace(/^\$\{\{\s*|\s*\}\}$/g, '');
  const checkoutRef = new Function('github', `return ${expression}`);
  for (const event of [{pull_request: {head: {sha: 'untrusted'}, base: {sha: 'old-base'}}}, {}]) {
    assert.equal(checkoutRef({sha: 'trusted-default-event', event}), 'trusted-default-event');
  }
});

test('dispatch precondition rejects non-default refs before checkout and unconfirmed cleanup', async () => {
  assert.equal(steps[0].name, 'Require default-branch manual dispatch');
  const run = new AsyncFunction('context', steps[0].with.script);
  const payload = {repository: {default_branch: 'master'}};
  await run({eventName: 'workflow_dispatch', ref: 'refs/heads/master', payload});
  await run({eventName: 'pull_request_target', ref: 'refs/heads/release-branch', payload});
  for (const ref of ['refs/heads/feature', 'refs/tags/v1.2.3']) {
    await assert.rejects(run({eventName: 'workflow_dispatch', ref, payload}), /repository default branch/);
  }
  const f = fixture({labels: [label, 'unrelated']});
  const {error} = await adapter(f, '', {CONFIRMED_PR: '', TARGET_OUTCOME: 'skipped',
    CHECKOUT_OUTCOME: 'skipped', INSTALL_OUTCOME: 'skipped', GUARD_OUTCOME: 'skipped',
    LABELER_OUTCOME: 'skipped'}, () => {throw new Error('Must not load helper');});
  assert.match(error.message, /not confirmed as a pull request/);
  assert.deepEqual([...f.labels], [label, 'unrelated']);
  assert.deepEqual(f.calls, []);
});


test('invalid PR revisions and identity fail before classification', async () => {
  for (const revision of [{head: {sha: ''}}, {head: {sha: 'changed'}}, {base: {sha: 'g'.repeat(40)}},
    {number: 21}, {changed_files: -1}, {changed_files: 1.5}, {changed_files: '1'}]) {
    await assert.rejects(guard.snapshot(fixture({revisions: [revision]}).github, repo, 20));
  }
});

test('trusted PR-event confirmation survives later lookup and setup failures', async () => {
  const f = fixture({failGetAt: 1, labels: [label, 'unrelated']});
  const target = await confirm(f, {repo, eventName: 'pull_request_target',
    payload: {pull_request: {number: 20}}});
  assert.ifError(target.error);
  const result = await adapter(f, '', {CONFIRMED_PR: target.outputs['confirmed-pr'], CHECKOUT_OUTCOME: 'failure'},
    () => {throw new Error('Helper is unavailable');});
  assert.match(result.error.message, /predecessor failed/);
  assert.deepEqual(f.calls, ['remove']);
  assert.deepEqual([...f.labels], ['unrelated']);
});
