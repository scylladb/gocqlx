// Copyright (C) 2026 ScyllaDB
const LABEL = 'omit-from-release-notes';

// Keep manifests, production sources/specifications, and unknown paths visible.
function supportPath(path) {
  if (typeof path !== 'string' || path.split('/').some(part => !part || part === '..' || part === '.')) {
    return false;
  }
  // gocqlxtest is an importable package, examples ship runnable code, and
  // migration example CQL is embedded by Go. Keep those and manifests visible.
  return /^\.github\//.test(path) || /_test\.go$/.test(path) ||
    /^(testdata|migrate\/testdata|cmd\/schemagen\/testdata)\//.test(path) ||
    ['README.md', 'migrate/README.md', 'qb/README.md',
      'examples/basic/README.md', 'RELEASING.md', '.gitignore',
      '.golangci.yml', 'renovate.json'].includes(path);
}

function validSnapshot(pr) {
  return pr && /^[0-9a-f]{40}$/i.test(pr.head?.sha || '') &&
    /^[0-9a-f]{40}$/i.test(pr.base?.sha || '') &&
    Number.isSafeInteger(pr.changed_files) && pr.changed_files > 0;
}

function complete(files, count) {
  return Number.isSafeInteger(count) && count > 0 && files.length === count &&
    new Set(files.map(file => file.filename)).size === count;
}

function supportOnly(files, count) {
  return complete(files, count) && files.every(file => supportPath(file.filename) &&
    (file.status !== 'renamed' || supportPath(file.previous_filename)) &&
    (file.previous_filename === undefined || supportPath(file.previous_filename)));
}

async function classify({github, context, core}, number) {
  const repo = context.repo;
  const issue = {...repo, issue_number: number};
  async function include() {
    try {
      await github.rest.issues.removeLabel({...issue, name: LABEL});
    } catch (error) {
      if (error.status !== 404) throw error;
    }
  }
  try {
    const before = (await github.rest.pulls.get({...repo, pull_number: number})).data;
    const files = await github.paginate(github.rest.pulls.listFiles,
      {...repo, pull_number: number, per_page: 100});
    const after = (await github.rest.pulls.get({...repo, pull_number: number})).data;
    if (!validSnapshot(before) || !validSnapshot(after) ||
        before.head.sha !== after.head.sha || before.base.sha !== after.base.sha ||
        before.changed_files !== after.changed_files || !complete(files, after.changed_files)) {
      throw new Error('PR changed during classification or file listing is incomplete/duplicated');
    }
    if (!supportOnly(files, after.changed_files)) {
      await include();
      core.info('PR remains included in release notes');
      return;
    }
    try {
      await github.rest.issues.createLabel({...repo, name: LABEL, color: 'ededed',
        description: 'Documentation, tests, or CI only; omit from generated release notes'});
    } catch (error) {
      if (error.status !== 422) throw error;
    }
    await github.rest.issues.addLabels({...issue, labels: [LABEL]});
    core.info('Support-only PR omitted from generated release notes');
  } catch (error) {
    try {
      await include();
    } catch (cleanupError) {
      core.warning(`Could not remove stale ${LABEL}: ${cleanupError.message}`);
    }
    throw error;
  }
}

module.exports = {LABEL, supportPath, supportOnly, classify};
