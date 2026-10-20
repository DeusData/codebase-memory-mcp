import { describe, expect, it, vi } from 'vitest';
import { nameOperations, operationChoices, operationLabels } from './operation-choices';
import type { SystemSymbol } from './system-architecture-source';

/*
 * Hand test 2026-10-04 (A4): the Behavior "Start" list of django-demo held 62 operations in the order the
 * server returned them, with look-alikes ("database_backwards · django/db/migrations/operations/models.py" six
 * times), and "handle · …/loaddata.py" could not be found.
 */
const operation = (id: number, name: string, file_path: string, more: Partial<SystemSymbol> = {}): SystemSymbol =>
    ({ id, name, qualified_name: '', label: '', file_path, component_id: '', ...more });
const MODELS = 'django/db/migrations/operations/models.py';
const scouted = [
    operation(1, 'main', 'django/conf/project_template/manage.py-tpl', { qualified_name: 'django-demo.django.conf.project_template.manage.main', start_line: 7 }),
    operation(2, 'delete_selected', 'django/contrib/admin/actions.py'),
    operation(3, 'handle', 'django/core/management/commands/makemigrations.py'),
    operation(4, 'handle', 'django/core/management/commands/loaddata.py'),
    operation(5, 'handle', 'django/core/management/commands/migrate.py'),
    operation(6, 'database_backwards', MODELS),
    operation(7, 'database_backwards', MODELS),
    operation(8, 'clean', 'django/forms/models.py'),
    operation(9, 'clean', 'django/forms/models.py'),
    operation(10, 'as_sql', 'django/db/models/fields/json.py'),
];
const labels = (list: { label: string }[]) => list.map(item => item.label);

describe('Behavior start choices (A4)', () => {
    it('lists every operation alphabetically by name, then by path', () => {
        const { all, total } = operationChoices(scouted, {});
        expect(total).toBe(10);
        expect(all.map(item => item.id)).toEqual([10, 8, 9, 6, 7, 2, 4, 3, 5, 1]);
        expect(labels(all).slice(5, 9)).toEqual(['delete_selected · django/contrib/admin/actions.py', 'handle · django/core/management/commands/loaddata.py',
            'handle · django/core/management/commands/makemigrations.py', 'handle · django/core/management/commands/migrate.py']);
    });

    it('puts the automatic start and the ranked suggestions first, in their order, and keeps them in the full list', () => {
        const { suggested, all } = operationChoices(scouted, { suggested: [1, 3, 4, 99] });
        expect(labels(suggested)).toEqual(['main · django/conf/project_template/manage.py-tpl', 'handle · django/core/management/commands/makemigrations.py',
            'handle · django/core/management/commands/loaddata.py']);
        expect(all).toHaveLength(10);
    });

    it('tells look-alikes apart by their class, by their line when the class is the same, and by position otherwise', () => {
        const named = scouted.map(item => item.id === 6 ? { ...item, qualified_name: `django-demo.django.db.migrations.operations.models.CreateModel.database_backwards`, start_line: 110 }
            : item.id === 7 ? { ...item, qualified_name: `django-demo.django.db.migrations.operations.models.DeleteModel.database_backwards`, start_line: 444 }
                : item.id === 8 ? { ...item, qualified_name: 'django-demo.django.forms.models.BaseModelForm.clean', start_line: 437 }
                    : item.id === 9 ? { ...item, qualified_name: 'django-demo.django.forms.models.BaseModelForm.clean', start_line: 800 } : item);
        const text = operationLabels(named);
        // The name stays first, so the list still reads alphabetically.
        expect(text.get(6)).toBe(`database_backwards (CreateModel) · ${MODELS}`);
        expect(text.get(7)).toBe(`database_backwards (DeleteModel) · ${MODELS}`);
        expect(text.get(8)).toBe('clean · django/forms/models.py:437');
        expect(text.get(9)).toBe('clean · django/forms/models.py:800');
        // Unique names keep their plain label.
        expect(text.get(4)).toBe('handle · django/core/management/commands/loaddata.py');
        // Without a qualified name or line the position still tells them apart.
        const bare = operationLabels(scouted);
        expect([bare.get(6), bare.get(7)]).toEqual([`database_backwards · ${MODELS} (1 of 2)`, `database_backwards · ${MODELS} (2 of 2)`]);
        expect(new Set(bare.values()).size).toBe(bare.size);
        // Sorted by name first, the class does not tear the look-alikes apart.
        expect(operationChoices(named, {}).all.map(item => item.id).slice(3, 5)).toEqual([6, 7]);
    });

    it('narrows to the operations that match every word of the filter, and keeps the chosen one', () => {
        expect(labels(operationChoices(scouted, { query: 'loaddata' }).all)).toEqual(['handle · django/core/management/commands/loaddata.py']);
        expect(operationChoices(scouted, { query: 'handle  MIGRATE' }).all.map(item => item.id)).toEqual([5]);
        // The chosen start that does not match stands apart (review of K43), so the matches hold what their count says.
        const kept = operationChoices(scouted, { query: 'loaddata', keep: 1, suggested: [1, 3] });
        expect(kept.all.map(item => item.id)).toEqual([4]);
        expect(kept.suggested.map(item => item.id)).toEqual([]);
        expect(kept.current?.id).toBe(1);
        expect(kept.matching).toBe(1);
        // A chosen start that matches counts as a match (measured in the browser: "0 of 61" after picking loaddata).
        expect(operationChoices(scouted, { query: 'loaddata', keep: 4 }).matching).toBe(1);
        expect(operationChoices(scouted, { query: 'nothing like this' }).all).toEqual([]);
    });
});

describe('qualified names for look-alike flow starts (A4)', () => {
    it('asks the index once for the look-alikes only and fills in their qualified name and line', async () => {
        const queryRows = vi.fn(async () => [
            { id: '6', qualified_name: 'django-demo.django.db.migrations.operations.models.CreateModel.database_backwards', start_line: '110' },
            { id: '7', qualified_name: 'django-demo.django.db.migrations.operations.models.DeleteModel.database_backwards', start_line: '444' },
            { id: '70', qualified_name: 'django-demo.django.db.migrations.operations.models.RenameModel.database_backwards', start_line: '538' },
            { id: '8', qualified_name: 'django-demo.django.forms.models.BaseModelForm.clean', start_line: '437' },
        ]);
        const named = await nameOperations('django-demo', scouted, { queryRows });
        expect(queryRows).toHaveBeenCalledOnce();
        const [project, query] = queryRows.mock.calls[0] as unknown as [string, string];
        expect(project).toBe('django-demo');
        expect(query).toContain('n.name IN ["database_backwards", "clean"]');
        expect(query).toContain(`n.file_path IN ["${MODELS}", "django/forms/models.py"]`);
        expect(query).not.toContain('handle');
        expect(named.find(item => item.id === 6)).toMatchObject({ qualified_name: 'django-demo.django.db.migrations.operations.models.CreateModel.database_backwards', start_line: 110 });
        expect(named.find(item => item.id === 9)?.qualified_name).toBe('');
        expect(named.find(item => item.id === 4)).toBe(scouted[3]);
    });

    it('needs no query without look-alikes, never puts quotes or backslashes into one, and keeps the list when it fails', async () => {
        const queryRows = vi.fn(async () => { throw new Error('index busy'); });
        expect(await nameOperations('p', scouted.slice(0, 5), { queryRows })).toEqual(scouted.slice(0, 5));
        expect(queryRows).not.toHaveBeenCalled();
        const odd = [operation(1, 'run"x', 'a.py'), operation(2, 'run"x', 'a.py')];
        expect(await nameOperations('p', odd, { queryRows })).toEqual(odd);
        expect(queryRows).not.toHaveBeenCalled();
        expect(await nameOperations('p', scouted, { queryRows })).toEqual(scouted);
    });
});
