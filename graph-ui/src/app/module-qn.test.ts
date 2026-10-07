/*
 * Die Ableitung Pfad -> qualifizierter Name, gegen die Namen geprueft, die ein
 * laufender Server am 2026-08-28 fuer fixtures/atlas-sample wirklich gefuehrt
 * hat (`MATCH (n:Module) RETURN n.qualified_name, n.file_path`, Projekt
 * `probe-small`). Die Faelle unten sind abgeschrieben, nicht ausgedacht: an
 * ihnen haengt, ob der Reader die richtige Datei laedt.
 */
import { describe, expect, it } from 'vitest';
import {
    baseName,
    fileQualifiedName,
    moduleQnFromFileQn,
    moduleQualifiedName,
    normalizeWorkspacePath,
    pathSegments,
    stripLastExtension,
} from './module-qn';

const PROJECT = 'probe-small';

describe('moduleQualifiedName', () => {

    it('bildet die Namen, die der Graph fuer das Fixture fuehrt', () => {
        expect(moduleQualifiedName(PROJECT, 'src/services/userService.ts'))
            .toBe('probe-small.src.services.userService');
        expect(moduleQualifiedName(PROJECT, 'src/repo/db.ts')).toBe('probe-small.src.repo.db');
        expect(moduleQualifiedName(PROJECT, 'src/types.ts')).toBe('probe-small.src.types');
    });

    it('nimmt genau eine Endung weg, nicht alle', () => {
        // Der Graph fuehrt diese Datei als probe-small.test.userService.test.
        expect(moduleQualifiedName(PROJECT, 'test/userService.test.ts'))
            .toBe('probe-small.test.userService.test');
    });

    it('behandelt eine Datei ohne Verzeichnis wie eine mit', () => {
        expect(moduleQualifiedName(PROJECT, 'HERKUNFT.md')).toBe('probe-small.HERKUNFT');
    });

    it('laesst einen Pfad ohne Endung stehen', () => {
        expect(moduleQualifiedName(PROJECT, 'scripts/Makefile')).toBe('probe-small.scripts.Makefile');
    });

    it('haengt sich nicht an einem Punkt im Verzeichnisnamen auf', () => {
        expect(moduleQualifiedName(PROJECT, '.github/workflows/ci.yml'))
            .toBe('probe-small..github.workflows.ci');
    });

    it('normalisiert fuehrende Schraegstriche und ./ weg', () => {
        expect(moduleQualifiedName(PROJECT, './src/types.ts')).toBe('probe-small.src.types');
        expect(moduleQualifiedName(PROJECT, '/src/types.ts')).toBe('probe-small.src.types');
    });
});

describe('stripLastExtension', () => {

    it('laesst einen Namen stehen, der nur aus einer Endung besteht', () => {
        // .gitignore ohne Endung waere ein leerer Name, und ein leerer Name ist
        // kein Symbol.
        expect(stripLastExtension('.gitignore')).toBe('.gitignore');
        expect(stripLastExtension('src/.gitignore')).toBe('src/.gitignore');
    });
});

describe('fileQualifiedName und der Rueckweg', () => {

    it('bildet den File-Knoten so, wie der Graph ihn fuehrt', () => {
        expect(fileQualifiedName(PROJECT, 'src/services/userService.ts'))
            .toBe('probe-small.src.services.userService.ts.__file__');
    });

    it('rechnet vom File-Knoten auf den Modul-Knoten zurueck', () => {
        expect(moduleQnFromFileQn('probe-small.src.services.userService.ts.__file__'))
            .toBe('probe-small.src.services.userService');
        expect(moduleQnFromFileQn('probe-small.test.userService.test.ts.__file__'))
            .toBe('probe-small.test.userService.test');
    });

    it('gibt nichts zurueck, wenn der Name kein File-Knoten ist', () => {
        expect(moduleQnFromFileQn('probe-small.src.services.userService')).toBeUndefined();
    });

    it('ist zur Ableitung aus dem Pfad deckungsgleich', () => {
        for (const path of ['src/services/userService.ts', 'test/userService.test.ts', 'HERKUNFT.md']) {
            expect(moduleQnFromFileQn(fileQualifiedName(PROJECT, path)))
                .toBe(moduleQualifiedName(PROJECT, path));
        }
    });
});

describe('Pfad-Hilfen der Anzeige', () => {

    it('zerlegt einen Pfad in die Segmente der Breadcrumb', () => {
        expect(pathSegments('src/services/userService.ts'))
            .toEqual(['src', 'services', 'userService.ts']);
    });

    it('nennt den Dateinamen fuer den Tab', () => {
        expect(baseName('src/services/userService.ts')).toBe('userService.ts');
        expect(baseName('package.json')).toBe('package.json');
    });

    it('normalisiert Rueckwaerts-Schraegstriche mit', () => {
        expect(normalizeWorkspacePath('src\\services\\userService.ts'))
            .toBe('src/services/userService.ts');
    });
});
