// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {supportOnly, classify} = require('./release-notes.cjs');
const file = filename => ({filename, status: 'modified'});

test('support-only mixtures qualify; production, manifests and unknown paths remain visible', () => {
  const files = ['README.md', 'migrate/README.md', 'qb/README.md',
    'examples/basic/README.md', 'queryx_test.go', 'qb/select_test.go',
    'testdata/people.json', 'migrate/testdata/file',
    'cmd/schemagen/testdata/go.mod', '.github/workflows/main.yml',
    '.golangci.yml', 'renovate.json'].map(file);
  assert.equal(supportOnly(files, files.length), true);
  for (const path of ['queryx.go', 'qb/select.go', 'go.mod', 'go.sum',
    'examples/basic/go.mod', 'examples/basic/go.sum', 'examples/basic/main.go',
    'Makefile', 'unknown.md', 'tools/build.sh', 'build/package.sh',
    'gocqlxtest/gocqlxtest.go', 'migrate/example/cql/embed.go',
    'migrate/example/cql/m1.cql', 'cmd/schemagen/keyspace.tmpl',
    'testdata/../queryx.go', '/README.md']) {
    assert.equal(supportOnly([...files, file(path)], files.length + 1), false, path);
  }
  assert.equal(supportOnly([], 0), false);
  assert.equal(supportOnly(files, files.length + 1), false);
  assert.equal(supportOnly([file('README.md'), file('README.md')], 2), false);
});

test('renames require both old and new support paths', () => {
  for (const [filename, previous_filename, expected] of [
    ['README.md', 'qb/README.md', true],
    ['README.md', 'conn.go', false],
    ['conn.go', 'README.md', false],
    ['README.md', undefined, false],
  ]) {
    assert.equal(supportOnly([{filename, previous_filename, status: 'renamed'}], 1), expected);
  }
});

function fixture(files, count = files.length) {
  const calls = [];
  const pull = {head: {sha: 'a'.repeat(40)}, base: {sha: 'b'.repeat(40)}, changed_files: count};
  const github = {
    rest: {
      pulls: {get: async () => ({data: pull}), listFiles: () => {}},
      issues: {
        createLabel: async args => calls.push(['create', args]),
        addLabels: async args => calls.push(['add', args]),
        removeLabel: async args => calls.push(['remove', args]),
      },
    },
    paginate: async (endpoint, args) => {
      assert.equal(endpoint, github.rest.pulls.listFiles);
      assert.equal(args.per_page, 100);
      calls.push(['paginate']);
      return files;
    },
  };
  return {github, context: {repo: {owner: 'scylladb', repo: 'gocqlx'}},
    core: {info: () => {}, warning: () => {}}, calls, pull};
}

test('pagination supports more than one page and creates a missing label', async () => {
  const f = fixture(Array.from({length: 201}, (_, i) => file(`testdata/data${i}.txt`)));
  await classify(f, 23);
  assert.deepEqual(f.calls.map(call => call[0]), ['paginate', 'create', 'add']);
});

test('existing label is reused', async () => {
  const f = fixture([file('README.md')]);
  f.github.rest.issues.createLabel = async () => {throw {status: 422};};
  await classify(f, 23);
  assert.equal(f.calls.at(-1)[0], 'add');
});

test('mixed PR removes stale omission label', async () => {
  const f = fixture([file('README.md'), file('go.mod')]);
  await classify(f, 23);
  assert.equal(f.calls.at(-1)[0], 'remove');
  assert.equal(f.calls.at(-1)[1].name, 'omit-from-release-notes');
});

test('incomplete/duplicated listing and API/write failures clean stale labels and fail visibly', async () => {
  for (const failure of ['truncated', 'duplicate', 'get', 'paginate', 'create', 'add']) {
    const f = fixture([file('README.md')]);
    if (failure === 'truncated') f.pull.changed_files = 2;
    if (failure === 'duplicate') {
      f.pull.changed_files = 2;
      f.github.paginate = async () => [file('README.md'), file('README.md')];
    }
    if (failure === 'get') f.github.rest.pulls.get = async () => {throw new Error('get');};
    if (failure === 'paginate') f.github.paginate = async () => {throw new Error('paginate');};
    if (failure === 'create') f.github.rest.issues.createLabel = async () => {throw new Error('create');};
    if (failure === 'add') f.github.rest.issues.addLabels = async () => {throw new Error('add');};
    await assert.rejects(classify(f, 23));
    assert.equal(f.calls.at(-1)[0], 'remove', failure);
  }
});

test('head, base and file-count changes invalidate a snapshot', async () => {
  for (const part of ['head', 'base', 'count']) {
    const f = fixture([file('README.md')]);
    let requests = 0;
    f.github.rest.pulls.get = async () => {
      const data = {...f.pull};
      if (++requests === 2) {
        if (part === 'count') data.changed_files++;
        else data[part] = {sha: 'changed'};
      }
      return {data};
    };
    await assert.rejects(classify(f, 23), /PR changed/);
    assert.equal(f.calls.at(-1)[0], 'remove');
  }
});

test('missing stale label is harmless; cleanup failures preserve the original failure', async () => {
  const f = fixture([file('go.mod')]);
  f.github.rest.issues.removeLabel = async () => {throw {status: 404};};
  await classify(f, 23);
  f.github.paginate = async () => {throw new Error('original');};
  f.github.rest.issues.removeLabel = async () => {throw new Error('cleanup');};
  await assert.rejects(classify(f, 23), /original/);
});


test('malformed PR snapshots fail visibly and remove stale labels', async () => {
  for (const mutate of [
    pr => {delete pr.head;},
    pr => {pr.base.sha = '';},
    pr => {pr.head.sha = 'not-a-sha';},
    pr => {pr.changed_files = 0;},
  ]) {
    const f = fixture([file('README.md')]);
    mutate(f.pull);
    await assert.rejects(classify(f, 23), /PR changed/);
    assert.equal(f.calls.at(-1)[0], 'remove');
  }
});

test('classifier workflow uses trusted code and one concurrency group for events and backfill', () => {
  const {readFileSync} = require('node:fs');
  const workflow = readFileSync('.github/workflows/label-release-notes.yml', 'utf8');
  assert.match(workflow, /pull_request_target:/);
  assert.match(workflow, /types: \[opened, synchronize, reopened, edited, closed\]/);
  assert.match(workflow, /workflow_dispatch:/);
  assert.match(workflow, /group: release-notes-\$\{\{ github.event.pull_request.number \|\| inputs.pr_number \}\}/);
  assert.match(workflow, /cancel-in-progress: false/);
  assert.match(workflow, /ref: \$\{\{ github.event.repository.default_branch \}\}/);
  assert.match(workflow, /persist-credentials: false/);
  assert.match(workflow, /pull-requests: read/);
  assert.match(workflow, /issues: write/);
  assert.doesNotMatch(workflow, /pull_request.head|contents: write/);
});
