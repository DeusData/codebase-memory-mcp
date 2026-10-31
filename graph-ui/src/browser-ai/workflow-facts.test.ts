import { describe, expect, it } from 'vitest';
import { workflowFacts, workflowFactLines } from './workflow-facts';

/** .github/workflows/new_contributor_pr.yml of django-demo, as Explore shows it. */
const NEW_CONTRIBUTOR = `name: New contributor message

on:
  pull_request_target:
    types: [opened]

permissions:
  pull-requests: write

jobs:
  build:
    name: Hello new contributor
    runs-on: ubuntu-latest
    steps:
      - uses: actions/first-interaction@v1
        with:
          repo-token: \${{ secrets.GITHUB_TOKEN }}
          pr-message: |
            Hello! Thank you for your contribution

            name: not a key
            jobs: not a key either
            - uses: not/a-step@v0

            Welcome aboard!
`;

describe('facts of a GitHub Actions workflow (K12)', () => {
    it('counts the one job, its trigger and the action it uses, not the text of a block value', () => {
        expect(workflowFacts(NEW_CONTRIBUTOR)).toEqual({
            name: 'New contributor message',
            triggers: [{ event: 'pull_request_target', types: ['opened'] }],
            jobs: [{ id: 'build', name: 'Hello new contributor', runsOn: 'ubuntu-latest', steps: 1 }],
            uses: ['actions/first-interaction@v1'],
        });
        expect(workflowFactLines('.github/workflows/new_contributor_pr.yml', NEW_CONTRIBUTOR)).toEqual([
            'Workflow name: `New contributor message`.',
            'Trigger: `pull_request_target` (types: opened).',
            '1 job: `build` ("Hello new contributor", runs on ubuntu-latest, 1 step).',
            'Actions used: `actions/first-interaction@v1`.',
        ]);
    });

    it('reads scalar and list triggers, compact step lists and several jobs', () => {
        const text = `name: "CI"
on: [push, pull_request]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - run: |
        echo one
      shell: bash
    - name: Test
      run: make test
  docs:
    uses: ./.github/workflows/docs.yml # reusable
`;
        expect(workflowFacts(text)).toEqual({
            name: 'CI',
            triggers: [{ event: 'push' }, { event: 'pull_request' }],
            jobs: [{ id: 'lint', runsOn: 'ubuntu-latest', steps: 3 }, { id: 'docs', steps: 0 }],
            uses: ['actions/checkout@v4', './.github/workflows/docs.yml'],
        });
        expect(workflowFactLines('.github/workflows/ci.yml', text)).toContain('2 jobs: `lint` (runs on ubuntu-latest, 3 steps); `docs` (0 steps).');
        expect(workflowFacts('on: push\njobs: {}\n')?.triggers).toEqual([{ event: 'push' }]);
    });

    it('says nothing for other files or text that is not a workflow', () => {
        expect(workflowFactLines('docker-compose.yml', NEW_CONTRIBUTOR)).toEqual([]);
        expect(workflowFactLines('.github/workflows/broken.yml', 'just text')).toEqual([]);
        expect(workflowFacts('')).toBeUndefined();
    });
});
