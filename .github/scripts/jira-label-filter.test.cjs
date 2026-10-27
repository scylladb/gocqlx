// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const yaml = require('js-yaml');
const workflow = yaml.load(readFileSync('.github/workflows/call_jira_sync.yml', 'utf8'));

// Evaluate the workflow's actual comparison predicates with GitHub's
// case-insensitive string equality; use no network or Jira calls.
function condition(expression, event) {
  const substituted = expression.replace(/github\.event\.(action|label\.name) (==|!=) '([^']*)'/g,
    (_, path, operator, literal) => {
      const actual = path === 'action' ? event.action : event.label?.name;
      const equal = typeof actual === 'string' && actual.toLowerCase() === literal.toLowerCase();
      return String(operator === '==' ? equal : !equal);
    });
  assert.match(substituted, /^(?:true|false|[&|()\s])+$/);
  return new Function(`return ${substituted}`)();
}

test('local Jira add/remove callers exclude only the complete logical omission marker', () => {
  for (const [id, action] of [['jira-sync-add-label', 'labeled'], ['jira-sync-remove-label', 'unlabeled']]) {
    const job = workflow.jobs[id];
    for (const name of ['omit-from-release-notes', 'Omit-From-Release-Notes', 'OMIT-FROM-RELEASE-NOTES']) {
      assert.equal(condition(job.if, {action, label: {name}}), false);
    }
    for (const name of ['bug', 'prefix-omit-from-release-notes', 'omit-from-release-notes-extra']) {
      assert.equal(condition(job.if, {action, label: {name}}), true);
      assert.equal(condition(job.if, {action: action === 'labeled' ? 'unlabeled' : 'labeled', label: {name}}), false);
    }
    assert.match(job.uses, /^scylladb\/github-automation\/\.github\/workflows\/main_jira_sync_(add|remove)_label\.yml@main$/);
  }
});

test('other local Jira event routes and secret forwarding remain intact', () => {
  for (const [id, expression] of Object.entries({
    'jira-sync-pr-opened': "github.event.action == 'opened' || github.event.action == 'edited'",
    'jira-sync-in-review': "github.event.action == 'ready_for_review' || github.event.action == 'review_requested'",
    'jira-sync-pr-closed': "github.event.action == 'closed'"
  })) assert.equal(workflow.jobs[id].if, expression);
  assert.deepEqual(workflow.on.pull_request_target.types,
    ['opened', 'edited', 'ready_for_review', 'review_requested', 'labeled', 'unlabeled', 'closed']);
  for (const job of Object.values(workflow.jobs)) {
    assert.equal(job.secrets.caller_jira_auth, '${{ secrets.USER_AND_KEY_FOR_JIRA_AUTOMATION }}');
  }
});
