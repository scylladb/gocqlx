// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {version, validateModule, publish} = require('./github-release.cjs');
const commit = 'a'.repeat(40);
const tag = 'v3.0.4';

test('accepts semantic version tags matching the Go module major', () => {
  for (const tag of ['v3.0.4', 'v3.1.0-rc.1']) {
    validateModule(tag, 'module github.com/scylladb/gocqlx/v3\n\ngo 1.25.0\n');
  }
  validateModule('v1.0.0', 'module github.com/scylladb/gocqlx\n');
  assert.equal(version('v3.1.0-rc.1').prerelease, true);
  for (const tag of ['3.0.4', 'v3.0', 'v03.0.4', 'v3.0.4-01', 'master']) {
    assert.throws(() => version(tag));
  }
  assert.throws(() => validateModule('v2.0.0', 'module github.com/scylladb/gocqlx/v3\n'), /major/);
});

function fixture(existing = false) {
  const calls = [];
  const object = {type: 'commit', sha: commit};
  const github = {rest: {
    git: {
      getRef: async args => {
        calls.push(['tag', args]);
        return {data: {object: {type: 'tag', sha: 'b'.repeat(40)}}};
      },
      getTag: async () => ({data: {object}}),
    },
    repos: {
      getContent: async args => {
        assert.equal(args.ref, commit);
        assert.equal(args.path, 'go.mod');
        return {data: {type: 'file', encoding: 'base64',
          content: Buffer.from('module github.com/scylladb/gocqlx/v3\n').toString('base64')}};
      },
      getReleaseByTag: async () => {
        if (!existing) throw {status: 404};
        return {data: {body: 'Curated notes', tag_name: tag, draft: false}};
      },
      generateReleaseNotes: async args => {
        calls.push(['notes', args]);
        return {data: {body: 'Filtered notes'}};
      },
      createRelease: async args => calls.push(['create', args]),
    },
  }};
  return {github, context: {repo: {owner: 'scylladb', repo: 'gocqlx'}},
    core: {info: () => {}}, calls, object};
}

test('verifies remote annotated tag and publishes filtered generated notes', async () => {
  const f = fixture();
  await publish(f, tag, commit);
  assert.deepEqual(f.calls.map(call => call[0]), ['tag', 'notes', 'create']);
  assert.equal(f.calls[0][1].ref, `tags/${tag}`);
  assert.equal(f.calls[1][1].configuration_file_path, '.github/release.yml');
  assert.equal(f.calls[2][1].body, 'Filtered notes');
  assert.equal(f.calls[2][1].tag_name, tag);
});

test('reruns preserve existing curated releases', async () => {
  const f = fixture(true);
  await publish(f, tag, commit);
  assert.deepEqual(f.calls.map(call => call[0]), ['tag']);
});

test('missing or mismatched authoritative tags prevent release creation', async () => {
  const f = fixture();
  f.object.sha = 'c'.repeat(40);
  await assert.rejects(publish(f, tag, commit), /does not match/);
  f.github.rest.git.getRef = async () => {throw {status: 404};};
  await assert.rejects(publish(f, tag, commit));
  assert.equal(f.calls.some(call => call[0] === 'create'), false);
});

test('release lookup errors are not mistaken for absent releases', async () => {
  const f = fixture();
  f.github.rest.repos.getReleaseByTag = async () => {throw new Error('Forbidden');};
  await assert.rejects(publish(f, tag, commit), /Forbidden/);
  assert.equal(f.calls.some(call => call[0] === 'create'), false);
});

test('workflow trusts default-branch automation and serializes each tag', () => {
  const workflow = readFileSync('.github/workflows/github-release.yml', 'utf8');
  assert.match(workflow, /ref: \$\{\{ github.event.repository.default_branch \}\}/);
  assert.match(workflow, /persist-credentials: false/);
  assert.match(workflow, /group: github-release-\$\{\{ github.ref \}\}/);
  assert.match(workflow, /cancel-in-progress: false/);
  assert.match(workflow, /context.ref.slice\('refs\/tags\/'.length\), context.sha/);
  assert.match(readFileSync('.github/release.yml', 'utf8'), /labels:\n      - omit-from-release-notes/);
});

test('existing draft or conflicting release is an actionable failure', async () => {
  for (const existing of [{tag_name: tag, draft: true},
    {tag_name: 'v3.0.3', draft: false}]) {
    const f = fixture(true);
    f.github.rest.repos.getReleaseByTag = async () => ({data: existing});
    await assert.rejects(publish(f, tag, commit), /unpublished or.*conflicting/);
    assert.equal(f.calls.some(call => call[0] === 'create'), false);
  }
});

test('invalid tagged module and unavailable module prevent release creation', async () => {
  for (const data of [{type: 'dir'}, {type: 'file', encoding: 'base64',
    content: Buffer.from('module github.com/scylladb/gocqlx/v2\n').toString('base64')}]) {
    const f = fixture();
    f.github.rest.repos.getContent = async () => ({data});
    await assert.rejects(publish(f, tag, commit));
    assert.equal(f.calls.some(call => call[0] === 'create'), false);
  }
});

test('lightweight tags work and prereleases are marked', async () => {
  const f = fixture();
  f.github.rest.git.getRef = async () => ({data: {object: f.object}});
  await publish(f, 'v3.1.0-rc.1', commit);
  assert.equal(f.calls.at(-1)[1].prerelease, true);
});

test('unresolved tag objects fail before release generation', async () => {
  const f = fixture();
  f.object.type = 'tag';
  await assert.rejects(publish(f, tag, commit), /does not match/);
  assert.equal(f.calls.some(call => call[0] === 'create'), false);
});
