import { selectionEvidenceContext } from '../galaxy/selection-evidence';
import type { BrowserChatContext } from './chat-model';

/** The django area in the Architecture overview of django-demo, shaped as SpatialArchitecture
 * publishes it: 24 documented members, hotspot findings and 24 connections with evidence. */
export function djangoAreaEvidence(): BrowserChatContext {
    const member = (index: number) => ({ id: 45000 + index, name: `Member${index}`, kind: 'Class', qualifiedName: `django-demo.django.apps.Member${index}`,
        filePath: `django/apps/member_${index}.py`, startLine: 13, endLine: 274, status: 'exported', incomingCalls: 9, outgoingCalls: 0,
        documentation: `"""Class ${index}. ${'A long docstring line. '.repeat(30)}"""`, packageName: 'apps' });
    const node = (name: string, path: string) => ({ id: name.length, name, kind: 'Function', qualifiedName: `django-demo.${name}`, filePath: path, startLine: 10, endLine: 20,
        documentation: `"""${'Documented. '.repeat(40)}"""` });
    const others = ['(root)', 'tests', 'docs'];
    const edges = Array.from({ length: 24 }, (_, index) => ({ id: `edge-${index}`, source: index % 5 === 4 ? others[index % 3] : 'django', target: index % 5 === 4 ? 'django' : others[index % 3],
        type: ['CALLS', 'USAGE', 'IMPORTS', 'INHERITS'][index % 4], count: index === 0 ? 1743 : 24 - index,
        evidence: Array.from({ length: 24 }, (_, item) => ({ source: node(`caller_${index}_${item}`, 'django/core/handlers.py'), target: node(`callee_${item}`, 'tests/test_x.py'),
            type: 'CALLS', line: 100 + item })), omittedEvidence: 3 }));
    return selectionEvidenceContext({
        project: 'django-demo', view: 'architecture-overview', source: 'indexed repository graph and architecture summary', generation: 'g1', label: 'django',
        selected: { id: 'area:django', kind: 'area', label: 'django', detail: '2310 files · 15299 indexed nodes', areaPath: 'django', count: 15299, memberCount: 15299,
            members: Array.from({ length: 24 }, (_, index) => member(index)), omittedMembers: 15275,
            measurement: { lines: 528578, measuredFiles: 2163, files: 2310, languages: [{ name: 'Unknown', color: '#81939e', lines: 358827, files: 1227 },
                { name: 'Python', color: '#79b7aa', lines: 158819, files: 883 }, { name: 'HTML', color: '#dfa184', lines: 3313, files: 162 }] },
            hotspots: { findings: [{ name: 'create', qualifiedName: 'django-demo.django.apps.config.AppConfig.create', filePath: 'django/apps/config.py', fanIn: 1278, line: 100 },
                { name: 'filter', qualifiedName: 'django-demo.django.db.models.query.QuerySet.filter', filePath: 'django/db/models/query.py', fanIn: 1224, line: 1487 },
                { name: 'reduce', qualifiedName: 'django-demo.django.db.migrations.operations.models.CreateModel.reduce', filePath: 'django/db/migrations/operations/models.py', line: 148, complexity: 27 }],
            maxFanIn: 1278, peakScore: 1 } },
        relationships: { count: 24, items: edges, omitted: 0 },
        scope: { view: 'overview', visibleNodes: 7, visibleEdges: 24 },
        limitations: { omittedNodes: 0, omittedEdges: 0, warnings: ['The loaded repository snapshot contains only part of the indexed graph.'],
            interpretation: 'Source areas group source locations. Hotspots measure static references, not runtime frequency. Relationships do not prove execution.' },
    });
}

/** `main` of manage.py-tpl as Behavior publishes it: two direct calls with call-site evidence. */
export function behaviorMainEvidence(currentSource?: string): BrowserChatContext {
    const main = { id: 46981, name: 'main', qualified_name: 'django-demo.django.conf.project_template.manage.main', label: 'Function',
        file_path: 'django/conf/project_template/manage.py-tpl', start_line: 7, end_line: 18, component_id: 'component-46980', group_id: 'directory:non_test:django' };
    return selectionEvidenceContext({
        project: 'django-demo', view: 'architecture-behavior', source: 'indexed behavior projection and call-site evidence', generation: 'g1', label: 'main',
        selected: { operation: main, caller: main, component: { id: 'component-46980', label: 'django/conf/project_template/manage.py-tpl', basis: 'declared_module', member_count: 3, file_count: 1, representatives: [] },
            currentSource: currentSource ? { key: 'k', source: { source: currentSource, file_path: '/abs/django/conf/project_template/manage.py-tpl', start_line: 7, end_line: 18 },
                provenance: 'Current local source; may differ from the indexed snapshot.' } : undefined },
        relationships: { count: 2, calls: [
            { id: 178383, source_id: 46981, target_id: 44220, type: 'CALLS', callsite: { file_path: 'django/conf/project_template/manage.py-tpl', line: 9 },
                arguments: [{ i: 0, e: "'DJANGO_SETTINGS_MODULE'" }, { i: 1, e: "'{{ project_name }}.settings'" }] },
            { id: 178384, source_id: 46981, target_id: 49233, type: 'CALLS', callsite: { file_path: 'django/conf/project_template/manage.py-tpl', line: 18 }, arguments: [{ i: 0, e: 'sys.argv' }] }] },
        scope: { entry: main, pathIndex: 0, step: 0, mode: 'immediate-calls' },
        limitations: { interpretation: 'Static call evidence only.' },
    });
}
