import { describe, expect, it } from 'vitest';

import { EXPLAIN_TAB_IDS, type ExplainFacts, explainTabOf, explainTabs } from './explain-tabs';

const QUIET: ExplainFacts = {
    hasProject: false,
    flowSubject: '',
    flowStep: -1,
    walkRunning: false,
    walkStep: 0,
    walkSteps: 0,
    chatTurns: 0,
};

describe('die Reiter des Erklaeren-Bereichs', () => {

    it('zeichnet immer alle Reiter, auch die leeren', () => {
        expect(explainTabs(QUIET).map((tab) => tab.id)).toEqual([...EXPLAIN_TAB_IDS]);
        expect(explainTabs({ ...QUIET, hasProject: true, chatTurns: 3 }).map((tab) => tab.id))
            .toEqual([...EXPLAIN_TAB_IDS]);
    });

    it('deaktiviert genau die Reiter, die nichts zu zeigen haben', () => {
        const tabs = explainTabs(QUIET);
        expect(tabs.filter((tab) => tab.enabled)).toHaveLength(0);
    });

    /*
     * Der Kern der Regel: ein deaktivierter Reiter verschwindet nicht, sondern
     * sagt, warum. Ohne diesen Test waere "gedimmt" eine Farbe ohne Aussage.
     */
    it('gibt jedem deaktivierten Reiter einen Grund, der ein Satz ist', () => {
        for (const tab of explainTabs(QUIET)) {
            expect(tab.enabled).toBe(false);
            expect(tab.reason.trim().length, `${tab.id} hat keinen Grund`).toBeGreaterThan(30);
            expect(tab.note.trim().length, `${tab.id} hat keine Zeile`).toBeGreaterThan(0);
        }
    });

    it('macht den Flow auf, sobald ein Symbol im Twin steht', () => {
        const tabs = explainTabs({ ...QUIET, flowSubject: 'createUser' });
        expect(explainTabOf(tabs, 'flow')?.enabled).toBe(true);
        expect(explainTabOf(tabs, 'flow')?.note).toContain('createUser');
    });

    it('macht den Walk erst auf, wenn wirklich eine Fuehrung laeuft', () => {
        expect(explainTabOf(explainTabs({ ...QUIET, walkRunning: true, walkSteps: 0 }), 'walk')?.enabled)
            .toBe(false);
        const running = explainTabs({ ...QUIET, walkRunning: true, walkStep: 2, walkSteps: 7 });
        expect(explainTabOf(running, 'walk')?.enabled).toBe(true);
        expect(explainTabOf(running, 'walk')?.note).toContain('3');
        expect(explainTabOf(running, 'walk')?.note).toContain('7');
    });

    it('zaehlt im Chat-Reiter die Fragen, die noch dastehen', () => {
        expect(explainTabOf(explainTabs({ ...QUIET, chatTurns: 1 }), 'chat')?.note)
            .toContain('1 question');
        expect(explainTabOf(explainTabs({ ...QUIET, chatTurns: 4 }), 'chat')?.note)
            .toContain('4 questions');
    });

    it('macht die beiden Assistenten an ein Projekt fest', () => {
        const withProject = explainTabs({ ...QUIET, hasProject: true });
        expect(explainTabOf(withProject, 'bug')?.enabled).toBe(true);
        expect(explainTabOf(withProject, 'change')?.enabled).toBe(true);
    });

    it('bleibt bei einem Schritt jenseits des Endes an der letzten Zahl', () => {
        const tabs = explainTabs({ ...QUIET, walkRunning: true, walkStep: 99, walkSteps: 4 });
        expect(explainTabOf(tabs, 'walk')?.note).toContain('4 of 4');
    });
});
