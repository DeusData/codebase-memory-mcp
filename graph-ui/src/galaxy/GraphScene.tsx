/*
 * MIT License. Copyright (c) 2025 DeusData.
 *
 * Uebernommen am 2026-08-28 aus DeusData/codebase-memory-mcp, Branch
 * feat/atlas-r1, Datei graph-ui/src/components/GraphScene.tsx. Der Lizenztext
 * und die Liste aller uebernommenen Dateien stehen in THIRD_PARTY.md.
 *
 * Aenderungen gegenueber dem Original:
 *  1. IdleAutoRotate suchte seinen Canvas per Dokumentabfrage nach dem
 *     Element-Namen und traf damit den ERSTEN Canvas des Dokuments, nicht den
 *     eigenen. In dieser Anwendung steht Monaco daneben, also greift die
 *     Szene jetzt ihren eigenen Canvas ueber useThree().gl.domElement, so wie
 *     es der ApproachWatcher des Originals schon tat.
 *  2. sceneRadius lief als IIFE bei jedem Render ueber alle Knoten. Jetzt in
 *     useMemo([data.nodes]): das Layout kommt fertig vom Server, der Radius
 *     aendert sich also genau dann, wenn die Knoten sich aendern.
 *  3. Der Satelliten-Zweig fuer data.linked_projects und der Geister-Zweig
 *     fuer `missed` sind gestrichen, samt der Props `missed`. Dieses Projekt
 *     zeigt eine Galaxie und laedt keine Fremdprojekte dazu; ein Zweig, den
 *     niemand einschaltet, waere toter Code mit eigenen Typen im Schlepptau.
 *  4. ApproachWatcher (Semantic Zoom) und ViewTargetReporter (Minimap-Feed)
 *     sind gestrichen, samt der Props onApproachNode und onViewTarget. Beide
 *     bedienen Panels, die dieses Projekt nicht uebernimmt.
 *  5. NodeTooltip war die einzige Tailwind-Abhaengigkeit der Uebernahme. Der
 *     Import ist weg; `renderTooltip` ist damit der einzige Weg zu einer
 *     Hover-Karte, und ohne die Prop zeigt die Szene keine. Die Karte dieses
 *     Projekts steht in NodeTooltipCard.tsx.
 *  6. Der Typ-Import von OrbitControlsImpl kommt weiter aus three-stdlib
 *     (hier als devDependency gefuehrt), React-Typen werden explizit
 *     importiert statt ueber den globalen React-Namensraum gelesen.
 *  7. Formatierung an den Stil dieses Projekts angeglichen (4 Leerzeichen,
 *     einfache Anfuehrungszeichen). Kein Verhalten haengt daran.
 *  8. Neu (W4e): die Prop `overlay`. Sie haengt einen beliebigen Knoten in die
 *     Szene, ohne dass die Szene wissen muss, was er zeigt. Gebraucht wird sie
 *     fuer den Ring um den Schritt, auf dem der Leser in der Hierarchie-Ansicht
 *     steht: ein Ring als Geometrie waere ein Neubau der Puffer bei jedem
 *     Schritt, und eine Animation davon einer bei jedem Bild. Ohne die Prop
 *     zeigt die Szene nichts zusaetzlich, also aendert sie den Galaxie-Modus
 *     nicht.
 *  9. Neu (W5c): die drei Label-Props `labelWorldFontSize`,
 *     `labelMaxTextWidth` und `onLabelLayout`. Sie werden unveraendert an
 *     NodeLabels durchgereicht; ohne sie zeichnet die Ebene wie das Original.
 *     Warum es sie gibt, steht im Kopf von NodeLabels.tsx.
 * 10. Neu (W10): vier Props fuer das, was Rechenzeit kostet, und was jede
 *     davon aendert:
 *      - `projection`: `flat` stellt eine orthografische Kamera von oben und
 *        nimmt den OrbitControls das Drehen. Schwenken und Zoomen bleiben.
 *      - `drawEdges`: `false` haengt die Kantenebene GAR NICHT ein. Ihre
 *        Helligkeit auf null zu setzen waere derselbe Rechenaufwand mit einem
 *        unsichtbaren Ergebnis, und ein Schalter, der spart, muss sparen.
 *      - `labelDistanceFactor`: Namen nur innerhalb dieses Vielfachen des
 *        Szenenradius. Bis W10 haengt die Sichtbarkeit rein an der
 *        Knotengroesse; die Kamera kam darin nicht vor.
 *      - `frameCap`: mit einem Deckel faehrt der Canvas mit
 *        `frameloop="never"` und wird von einem eigenen rAF-Takt bewegt.
 *        OHNE Deckel bleibt es bei `always`, also bei genau dem Renderloop,
 *        den die Beweislaeufe bis W9 gemessen haben.
 *     Dazu der Bildratenzaehler, der die Naht `globalThis.__atlasGalaxyPerf`
 *     fuellt (src/galaxy/frame-rate.ts). Er ist die EINZIGE Messung: das
 *     Panel liest denselben Wert, den er schreibt.
 * 11. Geaendert (W10): CameraAnimator zaehlt seinen Fortschritt nach der
 *     verstrichenen Zeit und nicht mehr je Bild. Bis dahin stand dort
 *     `progress.current += 0.02`, also fuenfzig Bilder bis ans Ziel; unter
 *     einem Bildratendeckel von 30 waere derselbe Anflug doppelt so lang
 *     gewesen. Bei 60 Bildern je Sekunde ist die Bewegung dieselbe wie vorher.
 * 12. Neu (W10b): ein Kameraziel darf eine Oben-Richtung mitbringen (`up`) und
 *     SOFORT gelten (`immediate`), und die Szene meldet an
 *     `globalThis.__atlasGalaxyFit`, wo ihre Knoten wirklich im Bild liegen.
 *     Alle drei gehoeren zur Einpassung aus AC5 und zu nichts sonst:
 *      - `up`, weil die eingepasste Ansicht senkrecht auf der groessten Flaeche
 *        der Wolke steht und dafuer eine eigene Oben-Richtung braucht
 *        (src/galaxy/camera-frame.ts). Ohne sie waere die Ausrichtung eine
 *        halbe: die Kamera stuende richtig und das Bild waere verdreht.
 *      - `immediate`, weil eine Einpassung keine Fahrt ist. Sie ist die Lage,
 *        in der ein Bild anfaengt, und ein Anflug darauf waere eine Bewegung
 *        ohne Aussage, die ausserdem jede Messung von einer Wartezeit abhaengig
 *        machte. Die Anfluege auf ein Symbol bleiben Anfluege.
 *      - die Naht, weil AC5 am BILD bewiesen wird und nicht an der Absicht: nur
 *        wer die Kamera hat, kann sagen, wo ein Knoten auf dem Schirm landet.
 * 13. Neu (W11b): ein Kameraziel darf mit einer kritisch gedaempften Feder
 *     angefahren werden (`spring`). Nur die FOLLOW-Kamera der Agentenebene
 *     benutzt es, und nur sie braucht es: sie bekommt waehrend eines Anflugs
 *     ein neues Ziel, und der Anflug oben faengt bei jedem neuen Ziel wieder
 *     bei null an, was genau der Ruck ist, den AC3c verbietet. Die Feder traegt
 *     ihre Geschwindigkeit ueber das neue Ziel hinweg; ihr Daempfungsgrad ist
 *     genau 1, sie kann also nicht ueberschwingen. Ohne das Feld ist jeder
 *     bestehende Anflug Zeichen fuer Zeichen der von vorher.
 */

import { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import type { JSX, ReactNode, RefObject } from 'react';
import { Canvas, useThree, useFrame } from '@react-three/fiber';
import { OrbitControls, OrthographicCamera } from '@react-three/drei';
import type { OrbitControls as OrbitControlsImpl } from 'three-stdlib';
import { EffectComposer, Bloom } from '@react-three/postprocessing';
import * as THREE from 'three';
import { NodeCloud } from './NodeCloud';
import { HaloLayer } from './HaloLayer';
import { EdgeLines } from './EdgeLines';
import { NodeLabels } from './NodeLabels';
import type { LabelBox } from './NodeLabels';
import { fitCamera, flatBounds, frameDistance, orthographicZoom } from './camera-frame';
import type { CameraFit, FrameBox } from './camera-frame';
import { FRAME_WINDOW_MS, recordFrameWindow, recordSceneFacts } from './frame-rate';
import { springStep } from '../agents/agent-motion';
import type { GraphData, GraphNode } from './types';
import {
    DEFAULT_DISPLAY_SETTINGS,
    bloomIntensityScale,
    nodeBoostScale,
    type DisplaySettings,
    type GraphProjection,
} from './density';

const BASE_BLOOM_INTENSITY = 1.45;

/* Camera fly-to animation */

interface CameraTarget {
    position: THREE.Vector3;
    lookAt: THREE.Vector3;
    /** Die Oben-Richtung der Kamera am Ziel. Fehlt sie, bleibt die bisherige. */
    up?: THREE.Vector3;
    /** Ohne Anflug setzen. Siehe Aenderung 12 im Kopf. */
    immediate?: boolean;
    /**
     * Mit einer kritisch gedaempften Feder statt mit dem Anflug (W11b).
     *
     * Aenderung 13 (W11b): der Anflug oben faengt bei jedem neuen Ziel wieder
     * bei null an. Fuer eine Kamera, die einem Agenten FOLGT, ist das der
     * falsche Anfang: sie ist noch unterwegs, wenn das naechste Ereignis kommt,
     * und ein Neustart des Fortschritts ist genau der Ruck, den AC3c dieses
     * Zyklus verbietet. Die Feder traegt ihre Geschwindigkeit ueber das neue
     * Ziel hinweg mit; kritisch gedaempft heisst, dass sie das Ziel dabei nicht
     * ueberschreitet. Ohne dieses Feld bleibt jeder bestehende Anflug genau der,
     * der er vor W11b war.
     */
    spring?: boolean;
}

/*
 * Aenderung 11 (W10): der Anflug rechnet in Sekunden statt in Bildern.
 *
 * Das Original addierte 0.02 je BILD und lerpte je Bild um denselben Bruchteil.
 * Beides haengt damit an der Bildrate: der Anflug dauerte fuenfzig Bilder, was
 * bei 60 Bildern je Sekunde 0.83 Sekunden sind und unter einem Deckel von 30
 * das Doppelte. Ein Bildratendeckel, der nebenbei die Animationen verlangsamt,
 * waere eine Einstellung mit einer zweiten, unangekuendigten Wirkung.
 *
 * Die beiden Konstanten sind die Umrechnung derselben Bewegung: 0.02 je Bild
 * bei 60 Bildern je Sekunde sind 1.2 je Sekunde, und der Bruchteil des Lerps
 * wird ueber `1 - (1 - a)^(delta * 60)` auf die verstrichene Zeit gehoben. Bei
 * genau 60 Bildern je Sekunde kommt beides auf dieselbe Zahl heraus wie vorher;
 * die Bewegung, die die Beweisbilder von W3 bis W9 zeigen, aendert sich nicht.
 */
const FLY_REFERENCE_FPS = 60;
const FLY_PROGRESS_PER_SECOND = 0.02 * FLY_REFERENCE_FPS;
const FLY_LERP_PER_FRAME = 0.08;
/* Ein einzelner sehr langer Bildabstand (ein Tab, der zurueckkommt) darf nicht
 * einen ganzen Anflug in einem Sprung erledigen. */
const FLY_MAX_DELTA = 0.1;

function CameraAnimator({
    target,
    controlsRef,
    flat = false,
}: {
    target: CameraTarget | null;
    controlsRef: RefObject<OrbitControlsImpl | null>;
    /* In der flachen Ansicht wird nur in der Ebene geflogen. Siehe useFrame. */
    flat?: boolean;
}) {
    const { camera } = useThree();
    const targetRef = useRef<CameraTarget | null>(null);
    const progress = useRef(1);
    /* Die Geschwindigkeit der Feder, je Achse, ueber Ziele hinweg. */
    const springVelocity = useRef({
        position: [0, 0, 0] as [number, number, number],
        lookAt: [0, 0, 0] as [number, number, number],
    });

    useEffect(() => {
        if (!target) {
            return;
        }
        targetRef.current = target;
        progress.current = 0;
        /*
         * Die Oben-Richtung wird SOFORT gesetzt, auch wenn geflogen wird.
         *
         * Sie ist keine Strecke, sondern die Frage, wo oben ist; sie waehrend
         * eines Anflugs zu drehen waere eine zweite Bewegung ueber der ersten.
         * OrbitControls liest `object.up` bei jedem `update()` neu (three-stdlib,
         * OrbitControls.update), also gilt sie ab dem naechsten Bild auch fuer
         * die Steuerung.
         */
        if (target.up !== undefined) {
            camera.up.copy(target.up).normalize();
        }
        if (target.immediate !== true) {
            return;
        }
        // Die Einpassung ist die Lage, in der das Bild anfaengt. Siehe Kopf.
        progress.current = 1;
        if (flat) {
            camera.position.x = target.lookAt.x;
            camera.position.y = target.lookAt.y;
        } else {
            camera.position.copy(target.position);
        }
        const controls = controlsRef.current;
        if (controls) {
            // In der flachen Ansicht bleibt der Drehpunkt auf z=0, sonst stuende
            // die Kamera von oben schief. Dieselbe Regel wie in FlatTarget.
            controls.target.set(target.lookAt.x, target.lookAt.y, flat ? 0 : target.lookAt.z);
            controls.update();
        } else {
            camera.lookAt(target.lookAt);
        }
    }, [target, camera, controlsRef, flat]);

    useFrame((_state, delta) => {
        if (!targetRef.current || progress.current >= 1) return;

        /*
         * Aenderung 13 (W11b): die Feder der FOLLOW-Kamera.
         *
         * Sie steht vor dem Anflug und nicht darin, damit an dem Weg, den jede
         * andere Fahrt dieses Panels nimmt, kein Zeichen anders ist. Die
         * Rechnung selbst liegt in src/agents/agent-motion.ts, weil der
         * Beweislauf sie nachrechnet.
         */
        const goal = targetRef.current;
        if (goal.spring === true) {
            const controls = controlsRef.current;
            const axes = ['x', 'y', 'z'] as const;
            let distance = 0;
            for (let i = 0; i < axes.length; i += 1) {
                const axis = axes[i] as 'x' | 'y' | 'z';
                const next = springStep(
                    { value: camera.position[axis], velocity: springVelocity.current.position[i] as number },
                    goal.position[axis],
                    delta,
                );
                camera.position[axis] = next.value;
                springVelocity.current.position[i] = next.velocity;
                distance += (goal.position[axis] - next.value) ** 2;
                if (controls) {
                    const pivot = springStep(
                        { value: controls.target[axis], velocity: springVelocity.current.lookAt[i] as number },
                        goal.lookAt[axis],
                        delta,
                    );
                    controls.target[axis] = pivot.value;
                    springVelocity.current.lookAt[i] = pivot.velocity;
                }
            }
            if (controls) {
                controls.update();
            } else {
                camera.lookAt(goal.lookAt);
            }
            /* Angekommen heisst: nah genug und langsam genug. */
            const speed = Math.hypot(...springVelocity.current.position);
            if (Math.sqrt(distance) < 0.5 && speed < 0.5) {
                progress.current = 1;
                springVelocity.current.position = [0, 0, 0];
                springVelocity.current.lookAt = [0, 0, 0];
            }
            return;
        }

        const step = Math.min(FLY_MAX_DELTA, Math.max(0, delta));
        progress.current = Math.min(1, progress.current + step * FLY_PROGRESS_PER_SECOND);
        const t = 1 - Math.pow(1 - progress.current, 3); /* ease-out cubic */
        const ease = 1 - Math.pow(1 - t * FLY_LERP_PER_FRAME, step * FLY_REFERENCE_FPS);

        /*
         * In der flachen Ansicht wird nur in der Ebene geflogen, und die Kamera
         * faehrt ueber den Blickpunkt statt neben ihn.
         *
         * `computeCameraTarget` setzt die Kamera bewusst schraeg versetzt
         * (x + d*0.2, y + d*0.15, z + d); in drei Dimensionen ist das die
         * bessere Ansicht. In einer Ansicht von oben waere derselbe Versatz eine
         * Schraeglage, und die Achse, die diese Ansicht fallen laesst, waere
         * wieder da. Die Entfernung bleibt unangetastet: bei einer
         * orthografischen Kamera aendert sie am Ausschnitt ohnehin nichts, aber
         * sie haelt die Wolke vor der vorderen Ebene.
         */
        if (flat) {
            const goal = targetRef.current;
            camera.position.x += (goal.lookAt.x - camera.position.x) * ease;
            camera.position.y += (goal.lookAt.y - camera.position.y) * ease;
            const flatControls = controlsRef.current;
            if (flatControls) {
                flatControls.target.x += (goal.lookAt.x - flatControls.target.x) * ease;
                flatControls.target.y += (goal.lookAt.y - flatControls.target.y) * ease;
                flatControls.update();
            }
            return;
        }

        camera.position.lerp(targetRef.current.position, ease);

        /* Move the OrbitControls pivot to the focus point as well. Otherwise the
         * controls keep their target at the origin and re-center the view on the
         * next frame, snapping the camera back to the middle after the fly-to. */
        const controls = controlsRef.current;
        if (controls) {
            controls.target.lerp(targetRef.current.lookAt, ease);
            controls.update();
        } else {
            camera.lookAt(targetRef.current.lookAt);
        }
    });

    return null;
}

/* Bildratenmesser und Bildratendeckel (W10) */

/**
 * Der Zaehler: Bilder je Fenster, gemeldet an src/galaxy/frame-rate.ts.
 *
 * Er rechnet nichts aus, was jemand anders auch rechnet. Das Panel zeigt genau
 * das Fenster, das hier gemeldet wird, und die Naht traegt dieselbe Zahl; eine
 * zweite Messung neben dieser waere eine zweite Wahrheit ueber dasselbe Bild.
 *
 * Das erste Bild nach dem Aufsetzen wird nicht mitgezaehlt: es enthaelt den
 * Aufbau der Szene und waere ein Ausreisser am Anfang jeder Messreihe.
 */
function FrameRateMeter({ nodes, edges, cap }: { nodes: number; edges: number; cap: number }): null {
    const frames = useRef(0);
    const windowStart = useRef(0);

    useEffect(() => {
        recordSceneFacts(nodes, edges, cap);
    }, [nodes, edges, cap]);

    useFrame(() => {
        const now = Date.now();
        if (windowStart.current === 0) {
            windowStart.current = now;
            frames.current = 0;
            return;
        }
        frames.current += 1;
        const elapsed = now - windowStart.current;
        if (elapsed >= FRAME_WINDOW_MS) {
            recordFrameWindow(frames.current, elapsed, now);
            frames.current = 0;
            windowStart.current = now;
        }
    });

    return null;
}

/** Wie viel frueher ein Bild noch als "im Takt" durchgeht. */
const FRAME_CAP_TOLERANCE_MS = 2;

/**
 * Der Deckel: der Canvas laeuft auf `never` und wird von hier getaktet.
 *
 * Der Weg ueber `advance()` und nicht ueber ein Ueberspringen in `useFrame`:
 * ein uebersprungenes `useFrame` spart nichts, denn der Canvas hat das Bild
 * schon gezeichnet, bevor der Rueckruf laeuft. Erst ein Renderloop, der gar
 * nicht laeuft, spart die Zeichenzeit, und `advance` ist der Weg, ihn
 * kontrolliert einen Schritt gehen zu lassen.
 *
 * Ohne Deckel gibt es diesen Takt nicht und der Canvas laeuft mit `always`,
 * also genau so wie vor W10. Eine Umstellung auch fuer den ungedeckelten Fall
 * waere eine Aenderung an einem Verhalten, das mehrere Beweislaeufe gemessen
 * haben, fuer eine Einstellung, die niemand eingeschaltet hat.
 */
function FrameCapDriver({ cap, active }: { cap: number; active: boolean }): null {
    const advance = useThree((state) => state.advance);

    useEffect(() => {
        if (!active || cap <= 0 || typeof requestAnimationFrame !== 'function') {
            return;
        }
        const interval = 1000 / cap;
        let last = 0;
        let handle = 0;
        const tick = (time: number): void => {
            handle = requestAnimationFrame(tick);
            if (time - last < interval - FRAME_CAP_TOLERANCE_MS) {
                return;
            }
            last = time;
            advance(time);
        };
        handle = requestAnimationFrame(tick);
        return () => cancelAnimationFrame(handle);
    }, [advance, active, cap]);

    return null;
}

/**
 * Die flache Ansicht: eine orthografische Kamera die z-Achse hinunter.
 *
 * Sie laesst die dritte Achse fallen, statt sie zu verkuerzen. Der Zoom kommt
 * aus `orthographicZoom` (src/galaxy/camera-frame.ts) und damit aus Pixeln je
 * Welteinheit; eine orthografische Kamera hat keinen Ausschnittkegel, also
 * braucht diese Rahmung keine Trigonometrie und die Entfernung der Kamera
 * aendert am Bild nichts. Sie steht trotzdem hinter der Wolke, damit nichts
 * hinter der vorderen Ebene abgeschnitten wird.
 */
/**
 * Der Drehpunkt der Steuerung, auf die Ebene z=0 gelegt.
 *
 * Ohne das bliebe er am Ursprung stehen, und OrbitControls zwingt die Kamera,
 * auf ihn zu sehen: eine Kamera bei (cx, cy, d), die auf (0, 0, 0) blickt,
 * steht schief. Das Ergebnis waere eine "Ansicht von oben", die schraeg ist,
 * und der ganze Unterschied dieser Einstellung waere weg.
 *
 * Als eigenes Kind der Szene und nicht als Effekt in GraphScene, weil die
 * Steuerung im Canvas haengt: der Canvas ist ein eigener Reconciler-Baum, und
 * ein Effekt ausserhalb liefe gegen ein Ref, das darin noch nicht gesetzt ist.
 */
function FlatTarget({
    box,
    controlsRef,
}: {
    box: FrameBox;
    controlsRef: RefObject<OrbitControlsImpl | null>;
}): null {
    useEffect(() => {
        const controls = controlsRef.current;
        if (controls === null) {
            return;
        }
        controls.target.set(box.centerX, box.centerY, 0);
        controls.update();
    }, [box, controlsRef]);
    return null;
}

function FlatCamera({ box, radius }: { box: FrameBox; radius: number }): JSX.Element {
    const size = useThree((state) => state.size);
    const zoom = useMemo(
        () => orthographicZoom(box, size.width, size.height),
        [box, size.width, size.height],
    );
    const distance = radius * 2 + 1000;
    return (
        <OrthographicCamera
            makeDefault
            position={[box.centerX, box.centerY, distance]}
            up={[0, 1, 0]}
            zoom={zoom}
            near={0.1}
            far={distance + radius * 4 + 1000}
        />
    );
}

/* Die Naht der Einpassung (W10b, AC5) */

/** Wo ein Knoten wirklich im Bild liegt, in Pixeln der Zeichenflaeche. */
export interface FitMeasurement {
    /** Wie viele Knoten gemessen wurden. */
    nodes: number;
    /** Wie viele davon im sichtbaren Bereich liegen. */
    inside: number;
    /** Wie viele nicht. Genau diese Zahl ist die Zusicherung aus AC5. */
    outside: number;
    /** Wie viele hinter der Kamera stehen. Auch das waere "nicht im Bild". */
    behind: number;
    /** Der kleinste Abstand eines Knotens zum Bildrand, in Pixeln. */
    marginPx: number;
    /** Das Rechteck, das die Knoten im Bild einnehmen. */
    box: { left: number; right: number; top: number; bottom: number };
    /** Wie viel des Bildes sie damit fuellen, in beiden Richtungen. */
    fill: { horizontal: number; vertical: number };
    /** Die Zeichenflaeche, so wie die Szene sie kennt. */
    viewport: { width: number; height: number };
    /** Wo die Kamera steht, wohin sie sieht und wo bei ihr oben ist. */
    camera: {
        position: [number, number, number];
        direction: [number, number, number];
        up: [number, number, number];
        fov: number;
        aspect: number;
    };
    /** Die fuenf Knoten, die dem Rand am naechsten stehen. */
    worst: { name: string; x: number; y: number; margin: number }[];
}

declare global {
    // eslint-disable-next-line no-var
    var __atlasGalaxyFit: { measure: () => FitMeasurement } | undefined;
}

/**
 * Die Messung, die AC5 verlangt: JEDER Knoten in den Bildraum gerechnet.
 *
 * Sie steht in der Szene, weil nur hier die Kamera steht, und sie ist eine
 * Funktion und kein Wert: fuenftausend Punkte bei jedem Bild zu projizieren
 * waere eine Messung, die das Gemessene aendert. Gerechnet wird erst, wenn
 * jemand fragt, und dann mit genau der Kamera, die gerade zeichnet.
 */
function FitProbe({ nodes }: { nodes: GraphNode[] }): null {
    const camera = useThree((state) => state.camera);
    const size = useThree((state) => state.size);

    useEffect(() => {
        const measure = (): FitMeasurement => {
            camera.updateMatrixWorld();
            const point = new THREE.Vector3();
            const view = new THREE.Vector3();
            const direction = new THREE.Vector3();
            camera.getWorldDirection(direction);
            let inside = 0;
            let outside = 0;
            let behind = 0;
            let marginPx = Number.POSITIVE_INFINITY;
            let left = Number.POSITIVE_INFINITY;
            let right = Number.NEGATIVE_INFINITY;
            let top = Number.POSITIVE_INFINITY;
            let bottom = Number.NEGATIVE_INFINITY;
            const seen: { name: string; x: number; y: number; margin: number }[] = [];
            for (const node of nodes) {
                point.set(node.x, node.y, node.z);
                view.copy(point).applyMatrix4(camera.matrixWorldInverse);
                point.project(camera);
                const x = ((point.x + 1) / 2) * size.width;
                const y = ((1 - point.y) / 2) * size.height;
                // z im Blickraum ist vor der Kamera negativ. Ein Punkt dahinter
                // hat auf dem Schirm keine Lage, die etwas bedeutet.
                if (view.z >= 0) {
                    behind += 1;
                    outside += 1;
                    marginPx = Math.min(marginPx, -1);
                    seen.push({ name: node.qualified_name ?? node.name, x, y, margin: -1 });
                    continue;
                }
                const margin = Math.min(x, size.width - x, y, size.height - y);
                marginPx = Math.min(marginPx, margin);
                left = Math.min(left, x);
                right = Math.max(right, x);
                top = Math.min(top, y);
                bottom = Math.max(bottom, y);
                if (margin >= 0) {
                    inside += 1;
                } else {
                    outside += 1;
                }
                seen.push({ name: node.qualified_name ?? node.name, x, y, margin });
            }
            seen.sort((a, b) => a.margin - b.margin);
            const visibleWidth = right - left;
            const visibleHeight = bottom - top;
            return {
                nodes: nodes.length,
                inside,
                outside,
                behind,
                marginPx: Number.isFinite(marginPx) ? Number(marginPx.toFixed(2)) : -1,
                box: {
                    left: Number.isFinite(left) ? Number(left.toFixed(2)) : 0,
                    right: Number.isFinite(right) ? Number(right.toFixed(2)) : 0,
                    top: Number.isFinite(top) ? Number(top.toFixed(2)) : 0,
                    bottom: Number.isFinite(bottom) ? Number(bottom.toFixed(2)) : 0,
                },
                fill: {
                    horizontal: size.width > 0 && Number.isFinite(visibleWidth)
                        ? Number((visibleWidth / size.width).toFixed(4)) : 0,
                    vertical: size.height > 0 && Number.isFinite(visibleHeight)
                        ? Number((visibleHeight / size.height).toFixed(4)) : 0,
                },
                viewport: { width: size.width, height: size.height },
                camera: {
                    position: [camera.position.x, camera.position.y, camera.position.z],
                    direction: [direction.x, direction.y, direction.z],
                    up: [camera.up.x, camera.up.y, camera.up.z],
                    fov: (camera as THREE.PerspectiveCamera).fov ?? 0,
                    aspect: size.height > 0 ? size.width / size.height : 0,
                },
                worst: seen.slice(0, 5),
            };
        };
        globalThis.__atlasGalaxyFit = { measure };
        return () => {
            globalThis.__atlasGalaxyFit = undefined;
        };
    }, [camera, size, nodes]);

    return null;
}

/* Idle auto-rotation */

const IDLE_TIMEOUT_MS = 60_000;
/* Der senkrechte Oeffnungswinkel der Kamera. Bis W5c stand die 50 nur unten im
 * Canvas; die Rahmung der Hierarchie rechnet mit ihr, und zwei Zahlen fuer
 * denselben Winkel waeren eine Kamera, die anders steht, als gerechnet wurde. */
export const GRAPH_CAMERA_FOV = 50;
export const GRAPH_CANVAS_DPR: [number, number] = [1, 1.5];
export const GRAPH_COMPOSER_MULTISAMPLING = 0;

function IdleAutoRotate({
    controlsRef,
    enabled = true,
}: {
    controlsRef: RefObject<OrbitControlsImpl | null>;
    /*
     * In der flachen Ansicht aus. Das Kreisen ist eine Drehung um die Achse,
     * die diese Ansicht gerade fallen laesst, und OrbitControls dreht dabei
     * auch dann, wenn das Drehen mit der Maus abgeschaltet ist: eine Karte von
     * oben, die sich von selbst zu drehen anfaengt, waere keine mehr.
     */
    enabled?: boolean;
}) {
    const { gl } = useThree();
    const lastInteraction = useRef(Date.now());

    /* Reset timer on any pointer/wheel event */
    const resetTimer = useCallback(() => {
        lastInteraction.current = Date.now();
        if (controlsRef.current) {
            controlsRef.current.autoRotate = false;
        }
    }, [controlsRef]);

    useEffect(() => {
        /* Aenderung 1: der eigene Canvas, nicht der erste im Dokument. */
        const canvas = gl.domElement;
        if (!canvas) return;

        canvas.addEventListener('pointerdown', resetTimer);
        canvas.addEventListener('wheel', resetTimer);
        return () => {
            canvas.removeEventListener('pointerdown', resetTimer);
            canvas.removeEventListener('wheel', resetTimer);
        };
    }, [gl, resetTimer]);

    useFrame(() => {
        if (!controlsRef.current) return;
        const idle = enabled && Date.now() - lastInteraction.current > IDLE_TIMEOUT_MS;
        controlsRef.current.autoRotate = idle;
    });

    return null;
}

/* Main scene */

interface GraphSceneProps {
    /* False pauses the render loop (hidden-but-mounted panel). */
    active?: boolean;
    data: GraphData;
    highlightedIds: Set<number> | null;
    cameraTarget: CameraTarget | null;
    showLabels: boolean;
    display?: DisplaySettings;
    onNodeClick: (node: GraphNode) => void;
    /* Fired when a click hits empty space (no node). */
    onBackgroundClick?: () => void;
    /* Hover card. Without it the scene shows none. */
    renderTooltip?: (node: GraphNode) => ReactNode;
    /* Landmark halos on the top hubs. */
    landmarks?: boolean;
    /* Anything else the caller wants inside the scene (Aenderung 8). */
    overlay?: ReactNode;
    /* Label geometry, passed straight through to NodeLabels (Aenderung 9). */
    labelWorldFontSize?: number | undefined;
    labelMaxTextWidth?: number | undefined;
    onLabelLayout?: ((boxes: LabelBox[]) => void) | undefined;
    /* Aenderung 10 (W10): was Rechenzeit kostet. Ohne diese vier zeichnet die
     * Szene wie vorher. */
    projection?: GraphProjection;
    /* False haengt die Kantenebene gar nicht ein. Siehe Aenderung 10. */
    drawEdges?: boolean;
    /*
     * Namen nur innerhalb dieses Vielfachen des Szenenradius. 0 heisst: keine
     * Grenze. Ein Vielfaches und keine Welteinheit, weil nur diese Datei den
     * Radius kennt: eine feste Entfernung waere in einem kleinen Projekt immer
     * an und in einem grossen immer aus. Der Grund steht auch bei
     * LABEL_DISTANCE_FACTORS in density.ts.
     */
    labelDistanceFactor?: number;
    /* Bilder je Sekunde, hoechstens. 0 heisst: kein Deckel. */
    frameCap?: number;
}

export type { CameraTarget };

export function GraphScene({
    active = true,
    data,
    highlightedIds,
    cameraTarget,
    showLabels,
    display = DEFAULT_DISPLAY_SETTINGS,
    onNodeClick,
    onBackgroundClick,
    renderTooltip,
    landmarks = false,
    overlay,
    labelWorldFontSize,
    labelMaxTextWidth,
    onLabelLayout,
    projection = 'spatial',
    drawEdges = true,
    labelDistanceFactor = 0,
    frameCap = 0,
}: GraphSceneProps) {
    const [hovered, setHovered] = useState<GraphNode | null>(null);
    const controlsRef = useRef<OrbitControlsImpl | null>(null);
    const flat = projection === 'flat';

    /* Adaptive density defaults x user multipliers. The automatic scale keeps
     * contrast roughly constant as the graph grows; the sliders nudge it.
     * NodeCloud applies `nodeBoost` directly (no internal density scaling),
     * whereas EdgeLines scales by edge density itself, so it receives only the
     * user edge-brightness multiplier to avoid double-applying. */
    /* Constrained camera: orbit bounds follow the scene's actual extent so
     * you can neither clip through the cloud nor zoom out into the void. */
    /* Aenderung 2: gemerkt statt bei jedem Render neu gerechnet. */
    const sceneRadius = useMemo(() => {
        let max = 100;
        for (const node of data.nodes) {
            const r = Math.sqrt(node.x * node.x + node.y * node.y + node.z * node.z);
            if (r > max) max = r;
        }
        return max;
    }, [data.nodes]);

    /* Das Rechteck der flachen Ansicht, gemerkt wie der Radius und aus
     * demselben Grund: es haengt nur an den Knoten. */
    const flatBox = useMemo(() => flatBounds(data.nodes), [data.nodes]);

    /*
     * Wie weit die Steuerung herauslassen muss (W10b).
     *
     * Bis W10b war die Grenze der dreifache Szenenradius, gemessen vom
     * URSPRUNG. Die eingepasste Ansicht steht aber so weit weg, dass die ganze
     * Wolke ins Bild passt, und in einem schmalen Panel ist das mehr: die
     * waagerechte Bedingung teilt durch das Seitenverhaeltnis. Eine Grenze
     * darunter waere eine Steuerung, die die eingepasste Ansicht sofort wieder
     * heranzieht, und der Leser saehe eine Kamera, die von selbst zurueckfaehrt.
     * Gerahmt wird die Ausdehnung der Wolke, nicht ihr Abstand vom Ursprung,
     * also geht die groessere der beiden Zahlen in die Grenze.
     */
    const orbitReach = useMemo(() => {
        let minX = Infinity;
        let maxX = -Infinity;
        let minY = Infinity;
        let maxY = -Infinity;
        let minZ = Infinity;
        let maxZ = -Infinity;
        for (const node of data.nodes) {
            minX = Math.min(minX, node.x);
            maxX = Math.max(maxX, node.x);
            minY = Math.min(minY, node.y);
            maxY = Math.max(maxY, node.y);
            minZ = Math.min(minZ, node.z);
            maxZ = Math.max(maxZ, node.z);
        }
        if (!Number.isFinite(minX)) {
            return 0;
        }
        return Math.sqrt((maxX - minX) ** 2 + (maxY - minY) ** 2 + (maxZ - minZ) ** 2);
    }, [data.nodes]);

    /* Aus dem Vielfachen wird hier eine Entfernung, weil hier der Radius steht. */
    const labelMaxDistance = labelDistanceFactor > 0 ? labelDistanceFactor * sceneRadius : 0;

    const nodeBoost = nodeBoostScale(data.nodes.length) * display.nodeGlow;
    const bloomIntensity =
        BASE_BLOOM_INTENSITY * bloomIntensityScale(data.nodes.length) * display.bloom;

    /*
     * Der Renderloop, in drei Lagen.
     *
     * `never` wenn das Panel zu ist (so war es schon), `never` mit eigenem Takt
     * wenn ein Deckel gilt (FrameCapDriver bewegt ihn dann), und sonst `always`
     * wie bisher. Die dritte Lage ist die Vorgabe, und sie ist unveraendert:
     * ein Umbau des Renderloops fuer alle waere eine Aenderung an dem, was die
     * Beweislaeufe von W3 bis W9 gemessen haben.
     */
    const frameloop = !active ? 'never' : frameCap > 0 ? 'never' : 'always';

    return (
        <Canvas
            frameloop={frameloop}
            camera={{ position: [0, 0, 800], fov: GRAPH_CAMERA_FOV, near: 0.1, far: 100000 }}
            style={{ background: '#0D0F12' }}
            dpr={GRAPH_CANVAS_DPR}
            gl={{
                antialias: false,
                alpha: false,
                powerPreference: 'high-performance',
            }}
            onPointerMissed={onBackgroundClick}
        >
            <color attach="background" args={['#0D0F12']} />
            <ambientLight intensity={0.5} />
            <pointLight position={[500, 500, 500]} intensity={0.6} />
            <pointLight
                position={[-300, -200, -300]}
                intensity={0.4}
                color="#6040ff"
            />

            {flat && <FlatCamera box={flatBox} radius={sceneRadius} />}

            {drawEdges && (
                <EdgeLines
                    nodes={data.nodes}
                    edges={data.edges}
                    highlightedIds={highlightedIds}
                    brightness={display.edgeBrightness}
                />
            )}
            <NodeCloud
                nodes={data.nodes}
                highlightedIds={highlightedIds}
                onHover={setHovered}
                onClick={onNodeClick}
                boost={nodeBoost}
            />
            {showLabels && (
                <NodeLabels
                    nodes={data.nodes}
                    highlightedIds={highlightedIds}
                    worldFontSize={labelWorldFontSize}
                    maxTextWidth={labelMaxTextWidth}
                    maxDistance={labelMaxDistance}
                    onLayout={onLabelLayout}
                />
            )}
            {landmarks && <HaloLayer nodes={data.nodes} />}

            {overlay}
            {hovered && renderTooltip !== undefined && renderTooltip(hovered)}

            <CameraAnimator target={cameraTarget} controlsRef={controlsRef} flat={flat} />
            <FitProbe nodes={data.nodes} />
            <IdleAutoRotate controlsRef={controlsRef} enabled={!flat} />
            <FrameRateMeter nodes={data.nodes.length} edges={data.edges.length} cap={frameCap} />
            <FrameCapDriver cap={frameCap} active={active} />

            <EffectComposer multisampling={GRAPH_COMPOSER_MULTISAMPLING}>
                <Bloom
                    luminanceThreshold={0.3}
                    luminanceSmoothing={0.7}
                    intensity={bloomIntensity}
                    mipmapBlur
                    radius={0.6}
                />
            </EffectComposer>

            {/*
              * Der `key` haengt an der Projektion, damit die Steuerung beim
              * Umschalten neu aufgesetzt wird: sie bindet sich beim Aufsetzen an
              * die Vorgabe-Kamera, und die ist nach einem Wechsel eine andere.
              * Ohne den Neuaufbau haenge sie an der Kamera von vorhin, und ein
              * Zug an der Maus bewegte die Ansicht, die gerade nicht zu sehen
              * ist.
              *
              * In der flachen Ansicht faellt das Drehen weg und nur das
              * Drehen: schwenken und zoomen sind der Weg durch eine Karte, und
              * eine Karte, die man drehen kann, ist keine Karte von oben mehr.
              */}
            <OrbitControls
                key={projection}
                ref={controlsRef}
                enableDamping
                enableRotate={!flat}
                dampingFactor={0.08}
                rotateSpeed={0.5}
                zoomSpeed={1.5}
                minDistance={Math.max(5, sceneRadius * 0.02)}
                maxDistance={Math.max(sceneRadius, orbitReach) * 4}
                autoRotateSpeed={0.4}
            />
            {flat && <FlatTarget box={flatBox} controlsRef={controlsRef} />}
        </Canvas>
    );
}

/*
 * Aenderung 9 (W5c): eine zweite Rahmung, frontal und formatfuellend.
 *
 * `computeCameraTarget` unten bleibt unveraendert und rahmt weiter die Wolke.
 * Diese hier rahmt ein Rechteck, und warum es sie gibt, steht im Kopf von
 * camera-frame.ts.
 */
export function computeFrameTarget(box: FrameBox, aspect: number): CameraTarget {
    const distance = frameDistance(box, GRAPH_CAMERA_FOV, aspect);
    return {
        position: new THREE.Vector3(box.centerX, box.centerY, distance),
        lookAt: new THREE.Vector3(box.centerX, box.centerY, 0),
        up: new THREE.Vector3(0, 1, 0),
        immediate: true,
    };
}

/*
 * Aenderung 12 (W10b): die dritte Rahmung, und die einzige, die auch die
 * RICHTUNG waehlt.
 *
 * Die beiden anderen setzen voraus, dass die Kamera schon frontal steht: sie
 * rahmen ein Rechteck in der Ebene z=0, und fuer die Hierarchie ist das richtig,
 * weil sie eine flache Zeichnung IST. Die Galaxie ist es nicht. Ihre Positionen
 * kommen dreidimensional vom Server und sind in den drei Richtungen
 * unterschiedlich weit; welche Richtung die duennste ist, weiss nur, wer die
 * Punkte anschaut. Genau das tut `fitCamera` (src/galaxy/camera-frame.ts), und
 * hier wird daraus ein Kameraziel.
 */
export function computeFitTarget(
    nodes: readonly { x: number; y: number; z: number }[],
    aspect: number,
): (CameraTarget & { fit: CameraFit }) | null {
    const fit = fitCamera(nodes, GRAPH_CAMERA_FOV, aspect);
    if (fit === null) {
        return null;
    }
    /*
     * Die gerechneten Zahlen reisen mit dem Ziel.
     *
     * Der Aufrufer schreibt sie in seinen Testgriff, und er soll sie nicht ein
     * zweites Mal rechnen muessen: zwei Rechnungen ueber dieselbe Einpassung
     * waeren zwei Wahrheiten, und die Stelle, an der sie auseinanderlaufen,
     * faellt niemandem auf.
     */
    return {
        position: new THREE.Vector3(fit.eye.x, fit.eye.y, fit.eye.z),
        lookAt: new THREE.Vector3(fit.center.x, fit.center.y, fit.center.z),
        up: new THREE.Vector3(fit.up.x, fit.up.y, fit.up.z),
        immediate: true,
        fit,
    };
}

/* Helper: compute camera target from node IDs */

export function computeCameraTarget(
    nodes: GraphNode[],
    ids: Set<number>,
): CameraTarget | null {
    if (ids.size === 0) return null;

    let cx = 0,
        cy = 0,
        cz = 0,
        count = 0;
    for (const node of nodes) {
        if (ids.has(node.id)) {
            cx += node.x;
            cy += node.y;
            cz += node.z;
            count++;
        }
    }
    if (count === 0) return null;

    cx /= count;
    cy /= count;
    cz /= count;

    /* Distance based on cluster spread: ensure we never zoom too close */
    let maxDist = 0;
    for (const node of nodes) {
        if (ids.has(node.id)) {
            const d = Math.sqrt(
                (node.x - cx) ** 2 + (node.y - cy) ** 2 + (node.z - cz) ** 2,
            );
            if (d > maxDist) maxDist = d;
        }
    }

    /* Minimum distance scales with count: single node = 300, cluster = spread-based */
    const spreadDist = maxDist * 3;
    const minDist = count <= 5 ? 300 : 200;
    const distance = Math.max(minDist, spreadDist);
    const lookAt = new THREE.Vector3(cx, cy, cz);
    const position = new THREE.Vector3(
        cx + distance * 0.2,
        cy + distance * 0.15,
        cz + distance,
    );

    return { position, lookAt };
}
