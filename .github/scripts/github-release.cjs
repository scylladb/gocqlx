// Copyright (C) 2026 ScyllaDB
function version(tag) {
  const match = /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/.exec(tag);
  if (!match || (match[4] || '').split('.').some(part => /^0\d+$/.test(part))) {
    throw new Error('Expected a Go semantic version tag such as v3.0.4');
  }
  return {major: match[1], prerelease: Boolean(match[4])};
}

function validateModule(tag, goMod) {
  const {major} = version(tag);
  const module = /^module\s+(\S+)\s*$/m.exec(goMod)?.[1];
  const expected = `github.com/scylladb/gocqlx${Number(major) >= 2 ? `/v${major}` : ''}`;
  if (module !== expected) throw new Error('Release tag major does not match the Go module path');
}

async function publish({github, context, core}, tag, expectedCommit) {
  const {prerelease} = version(tag);
  if (!/^[0-9a-f]{40}$/i.test(expectedCommit)) throw new Error('Invalid release commit');
  const repo = context.repo;
  let object = (await github.rest.git.getRef({...repo, ref: `tags/${tag}`})).data.object;
  for (let depth = 0; object.type === 'tag' && depth < 10; depth++) {
    object = (await github.rest.git.getTag({...repo, tag_sha: object.sha})).data.object;
  }
  if (object.type !== 'commit' || object.sha !== expectedCommit) {
    throw new Error('Published tag does not match the triggering release commit');
  }
  const moduleFile = (await github.rest.repos.getContent({...repo, path: 'go.mod',
    ref: expectedCommit})).data;
  if (moduleFile.type !== 'file' || moduleFile.encoding !== 'base64') {
    throw new Error('Could not read the tagged Go module');
  }
  validateModule(tag, Buffer.from(moduleFile.content, 'base64').toString('utf8'));
  try {
    const existing = (await github.rest.repos.getReleaseByTag({...repo, tag})).data;
    if (existing.draft || existing.tag_name !== tag) {
      throw new Error('Existing release is unpublished or has a conflicting tag');
    }
    core.info(`Release ${tag} already exists; preserving its notes`);
    return;
  } catch (error) {
    if (error.status !== 404) throw error;
  }
  const notes = (await github.rest.repos.generateReleaseNotes({...repo, tag_name: tag,
    configuration_file_path: '.github/release.yml'})).data;
  await github.rest.repos.createRelease({...repo, tag_name: tag,
    name: tag, body: notes.body, draft: false, prerelease});
}

module.exports = {version, validateModule, publish};
