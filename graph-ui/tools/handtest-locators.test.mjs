import { expect, it } from 'vitest';
import { JSDOM } from 'jsdom';
import { architectureNodeSelector, projectNamePattern } from './handtest-locators.mjs';

it.each(['node', 'node"name', 'node\\name', 'node\\"name', 'node\nname', 'node\rname', 'node\fname', 'node], button'])('selects only the literal graph node ID: %j', id => {
    const { document } = new JSDOM('<button class="architecture-node-label" data-node-id="other"></button>').window;
    const target = document.createElement('button');
    target.className = 'architecture-node-label';
    target.setAttribute('data-node-id', id);
    document.body.append(target);
    expect(document.querySelectorAll(architectureNodeSelector(id))).toHaveLength(1);
    expect(document.querySelector(architectureNodeSelector(id))).toBe(target);
});

it.each(['a.b', 'a+b', '(project)', '[project]', 'a\\b', '*', '(a+)+$'])('matches a complete literal project name: %j', name => {
    const pattern = projectNamePattern(name);
    expect(pattern.test(name)).toBe(true);
    expect(pattern.test(`prefix${name}`)).toBe(false);
    expect(pattern.test(`${name}suffix`)).toBe(false);
});

it('does not treat a project-name dot as a wildcard', () => {
    expect(projectNamePattern('a.b').test('axb')).toBe(false);
});
