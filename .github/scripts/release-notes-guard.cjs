// Copyright (C) 2026 ScyllaDB
const {createHash} = require('node:crypto');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const yaml = require('js-yaml');
const {Minimatch} = require('minimatch');

const label = 'omit-from-release-notes';

function parseNumber(value) {
  if (typeof value !== 'string' || !/^[1-9][0-9]*$/.test(value) ||
      !Number.isSafeInteger(Number(value))) {
    throw new Error('Expected a canonical positive safe PR number');
  }
  return Number(value);
}

function policy(source = readFileSync(join(__dirname, '../labeler.yml'), 'utf8')) {
  const config = yaml.load(source);
  const onlyKey = (object, key) => object && !Array.isArray(object) &&
    typeof object === 'object' && Object.keys(object).length === 1 &&
    Object.hasOwn(object, key);
  const rules = config?.[label];
  const files = rules?.[0]?.['changed-files'];
  const globs = files?.[0]?.['all-globs-to-all-files'];
  if (!onlyKey(config, label) || !Array.isArray(rules) || rules.length !== 1 ||
      !onlyKey(rules[0], 'changed-files') || !Array.isArray(files) || files.length !== 1 ||
      !onlyKey(files[0], 'all-globs-to-all-files') || !Array.isArray(globs) ||
      globs.length === 0 || !globs.every(glob => typeof glob === 'string' && glob.length > 0)) {
    throw new Error('Unsupported labeler configuration; update the guard before changing its shape');
  }
  const matchers = globs.map(glob => new Minimatch(glob, {dot: true}));
  return filename => matchers.every(matcher => matcher.match(filename));
}

function revision(pr) {
  if (!/^[0-9a-f]{40}$/i.test(pr.head?.sha ?? '') ||
      !/^[0-9a-f]{40}$/i.test(pr.base?.sha ?? '') || !Number.isSafeInteger(pr.changed_files) ||
      pr.changed_files <= 0) throw new Error('Invalid PR revision or changed-file count');
  return {head: pr.head.sha, base: pr.base.sha, count: pr.changed_files};
}

async function readRevision(github, params) {
  const pr = (await github.rest.pulls.get(params)).data;
  if (pr.number !== params.pull_number) throw new Error('Invalid PR identity');
  return revision(pr);
}

async function snapshot(github, repo, number) {
  const params = {...repo, pull_number: number};
  const before = await readRevision(github, params);
  const files = await github.paginate(github.rest.pulls.listFiles, {...params, per_page: 100});
  const seen = new Set();
  const records = files.map(file => {
    if (typeof file.filename !== 'string' || !file.filename || seen.has(file.filename) ||
        !['added', 'removed', 'modified', 'renamed', 'copied', 'changed', 'unchanged'].includes(file.status) ||
        (file.previous_filename !== undefined &&
          (typeof file.previous_filename !== 'string' || !file.previous_filename)) ||
        (file.status === 'renamed' && !file.previous_filename)) {
      throw new Error('Incomplete or duplicate changed-file metadata');
    }
    seen.add(file.filename);
    return {filename: file.filename, status: file.status, previous_filename: file.previous_filename ?? null};
  }).sort((a, b) => a.filename < b.filename ? -1 : a.filename > b.filename ? 1 : 0);
  const after = await readRevision(github, params);
  if (JSON.stringify(before) !== JSON.stringify(after)) throw new Error('PR revisions changed during file collection');
  if (records.length !== before.count) throw new Error('Changed-file listing does not match expected count');
  const matches = policy();
  const unsafeRename = records.some(file => file.previous_filename && !matches(file.previous_filename));
  const omit = !unsafeRename && records.every(file => matches(file.filename));
  const hash = createHash('sha256').update(JSON.stringify({...before, files: records})).digest('hex');
  return {...before, hash, omit, unsafeRename};
}

async function verify(github, repo, number, expectedHash) {
  const current = await snapshot(github, repo, number);
  if (current.hash !== expectedHash) throw new Error('PR file snapshot changed since classification');
  const labels = await github.paginate(github.rest.issues.listLabelsOnIssue,
    {...repo, issue_number: number, per_page: 100});
  if (!labels.every(item => item && typeof item.name === 'string' && item.name.length > 0)) {
    throw new Error('Invalid label metadata');
  }
  if (labels.some(item => item.name.replace(/[A-Z]/g, character => character.toLowerCase()) === label) !== current.omit) {
    throw new Error('Actual omission label does not match verified file policy');
  }
  const after = await readRevision(github, {...repo, pull_number: number});
  if (JSON.stringify(after) !== JSON.stringify({head: current.head, base: current.base, count: current.count})) {
    throw new Error('PR revisions changed during label verification');
  }
  return current;
}

module.exports = {parseNumber, policy, snapshot, verify};
