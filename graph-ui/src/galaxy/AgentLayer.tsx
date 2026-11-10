/**
 * Die Agentenebene: kleine leuchtende Koerper, die den Knoten umkreisen, an dem
 * ein Agent gerade arbeitet, und die zum naechsten FLIEGEN statt dorthin zu
 * springen.
 *
 * ## Warum eine eigene Ebene und keine Umfaerbung
 *
 * Weil die Farben dieses Graphen schon etwas bedeuten. Die Knoten sind
 * Sternklassen nach ihrem Grad, die Kanten tragen seit W9 zwoelf
 * unterscheidbare Arten, und die Legende erklaert beides. Einen Knoten
 * umzufaerben, weil gerade jemand daran arbeitet, hiesse, eine Aussage ueber
 * die Struktur durch eine ueber die Gegenwart zu ersetzen: der Leser saehe
 * einen Stern der falschen Klasse und wuesste nicht, dass er es tut. Diese
 * Ebene legt sich DARUEBER und laesst jedes Pixel des Graphen, wie es war.
 *
 * ## Warum die Koerper DOM sind und keine Geometrie
 *
 * Dieselbe Entscheidung wie beim Ring der Hierarchie-Ansicht (Entscheidung 9 im
 * Kopf von GalaxyPanel.tsx), und hier kommt ein zweiter Grund dazu: die Groesse
 * steht im Contract in PIXELN (8 bis 12). Ein Objekt in der Szene hat eine
 * Groesse in Welteinheiten, und wie viele Pixel daraus werden, entscheidet die
 * Kamera. Ein DOM-Element ist genau so gross, wie es gross sein soll, und es
 * traegt seinen Buchstaben lesbar daneben, was eine Kugel von zehn Pixeln nicht
 * kann.
 *
 * Bewegt wird trotzdem in der Szene: das `group`, in dem der Koerper haengt,
 * bekommt in jedem Bild seine Position, und das DOM-Element folgt der
 * Weltmatrix. Ein Umweg ueber den React-Zustand waere ein Neurendern des Baums
 * bei jedem Bild.
 *
 * ## Was die vier Arten unterscheidet, und woran man es sieht
 *
 *  - **lesen**: weiter Orbit, ruhig. Ein Agent, der liest, geht nicht an eine
 *    Stelle, er sieht sich um.
 *  - **schreiben**: enger Orbit, schneller. Er ist AN der Stelle.
 *  - **suchen**: weiter Orbit, und dazu kurze Pings an den Knoten, deren NAME
 *    das Suchmuster traegt. Was eine Suche wirklich gelesen hat, weiss dieses
 *    Fenster nicht; was es weiss, ist, welche Symbole so heissen, und genau das
 *    zeigen die Pings.
 *  - **testen**: eine gestrichelte Linie zum geprueften Bereich, wenn der
 *    Befehl eine Datei nennt, die der Index kennt. Nennt er keine, gibt es
 *    keine Linie, und das Instrument sagt warum.
 *
 * ## Was W11b dazugelegt hat, und die eine Regel dahinter
 *
 * **Jede Bewegung hier ist ein Ereignis.** Nichts bewegt sich, weil die Uhr
 * laeuft.
 *
 *  1. **Der Flug.** Wechselt der Ort eines Akteurs, faellt sein Koerper nicht
 *     an die neue Stelle, er fliegt in {@link TRANSITION_MS} Millisekunden auf
 *     einer gebogenen Bahn dorthin. Die Kruemmung ist nicht Zierde: an einer
 *     Geraden sieht man nicht, in welche Richtung geflogen wurde, und zwei
 *     Fluege zwischen denselben Symbolen zeichnen dieselbe Strecke. Hinter dem
 *     Koerper laeuft ein kurzer Schweif nach, der sagt, wie schnell es ging.
 *  2. **Die Spur.** Die zuletzt besuchten Knoten bleiben gestrichelt
 *     verbunden stehen, UNTER den echten Kanten des Graphen und in der Farbe
 *     des Akteurs. Sie ist der Unterschied zwischen "folgt einem Aufrufpfad"
 *     und "springt quer durch das Repository", und sie ist gestrichelt und
 *     tieferliegend, damit sie nie fuer eine Beziehung im Code gehalten wird.
 *     Die Legende sagt denselben Unterschied noch einmal in Worten.
 *  3. **Der Puls.** Der Koerper atmet im Takt der Ereignisse dieses Akteurs:
 *     viele in der letzten Minute ergeben einen schnellen, kraeftigen Puls,
 *     wenige einen langsamen, schwachen, keine gar keinen. In einem Bild mit
 *     tausenden leuchtenden Knoten ist ein weiterer leuchtender Punkt kein
 *     Unterschied; ein Punkt, der atmet, ist einer. Und weil der Takt die
 *     gezaehlten Ereignisse SIND, ist der Puls selbst eine Auskunft und keine
 *     Dekoration.
 *  4. **Die Welle.** Ein Schreib-Bruch (mehrere Aenderungen am selben Knoten in
 *     kurzer Folge) erzeugt GENAU EINE konzentrische Welle. Eine je Ereignis
 *     waere Dauerfeuer, und Dauerfeuer sagt nichts.
 *  5. **Die Ruhe.** Ein Akteur ohne Ereignis seit einer Minute haelt an: kein
 *     Orbit, kein Puls, keine Pings, blass. Er verschwindet nicht, denn sein
 *     Lauf laeuft. Das ist AC6 dieses Zyklus, und es ist die Bedingung dafuer,
 *     dass die vier Punkte darueber ueberhaupt etwas bedeuten.
 *
 * ## Die Nahtstellen fuer den Beweislauf
 *
 * {@link agentAngles} traegt den Winkel jedes Koerpers, {@link agentPositions}
 * seine Weltposition, {@link agentMotion} die aufgezeichneten Punkte des
 * letzten Fluges, {@link agentTails} die Zahl der Schweifpunkte,
 * {@link agentCamera} die Lage der Kamera. Alle fuenf sind LESUNGEN aus dem
 * Bild, das gerade gezeichnet wurde, und keine zweite Rechnung daneben: was der
 * Beweislauf misst, ist genau die Zahl, die den Koerper bewegt hat.
 */

import type { JSX } from 'react';
import { useEffect, useMemo, useRef } from 'react';
import { Html } from '@react-three/drei';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';

import { hashOf } from '../agents/agent-colors';
import type { ActorView } from '../agents/agent-view';
import type { WorkKind } from '../agents/agent-event';
import { agentStrings as text } from '../agents/agent-strings';
import {
    COMET_TAIL_FADE_MS,
    COMET_TAIL_POINTS,
    TRAIL_SEGMENT_CAP,
    TRANSITION_MS,
    bendSignOf,
    controlPointOf,
    distanceFromChord,
    easeInOutCubic,
    pulseScaleAt,
    transitionPointAt,
} from '../agents/agent-motion';
import type { MotionPoint } from '../agents/agent-motion';

/** Wie ein Koerper kreist, je Art der Arbeit. */
export interface OrbitShape {
    /** Der Bahnradius, in Welteinheiten. */
    radius: number;
    /** Wie lange eine Umkreisung dauert, in Millisekunden. */
    periodMs: number;
}

/**
 * Die vier Bahnen, und die eine fuer alles, was sich nicht zuordnen laesst.
 *
 * Die Zahlen sind eine Wahl und keine Messung, aber sie folgen einer Regel:
 * "enger" heisst kleiner als der Lese-Radius, und "ruhiger" heisst eine
 * laengere Umlaufzeit. Der Leser soll den Unterschied ohne Legende sehen, also
 * ist er gross: der Schreib-Radius ist weniger als die Haelfte des Lese-Radius,
 * die Umlaufzeit weniger als die Haelfte.
 */
export const ORBITS: Readonly<Record<WorkKind, OrbitShape>> = {
    read: { radius: 34, periodMs: 7000 },
    write: { radius: 15, periodMs: 2600 },
    search: { radius: 34, periodMs: 4200 },
    test: { radius: 18, periodMs: 3200 },
    other: { radius: 24, periodMs: 5200 },
};

/**
 * Die Zeichenreihenfolge der Spur.
 *
 * Negativ, damit sie VOR allem anderen gezeichnet wird und damit unter ihm
 * liegt. Das ist die harte Haelfte der Zusicherung aus AC2; die weiche ist der
 * gestrichelte Strich, und die dritte ist der Satz in der Legende.
 */
export const TRAIL_RENDER_ORDER = -2;

/**
 * Der Winkel jedes Koerpers, in Grad, aus dem zuletzt gezeichneten Bild.
 *
 * Ein Modulwert und kein Zustand: er aendert sich bei jedem Bild, und ein
 * React-Zustand daraus waere ein Neurendern des Baums sechzig Mal je Sekunde
 * fuer eine Zahl, die nichts zeichnet.
 */
export const agentAngles: Record<string, number> = {};

/** Die Weltposition jedes Koerpers aus demselben Bild. */
export const agentPositions: Record<string, MotionPoint> = {};

/** Wie gross der Koerper in diesem Bild gezeichnet wurde, als Vielfaches. */
export const agentPulseScale: Record<string, number> = {};

/** Wie viele Punkte der Schweif dieses Akteurs gerade traegt. */
export const agentTails: Record<string, number> = {};

/** Ein aufgezeichneter Punkt eines Fluges. */
export interface MotionSample extends MotionPoint {
    /** Millisekunden seit dem Beginn des Fluges. */
    t: number;
}

/** Ein Flug, so wie er wirklich geflogen wurde. */
export interface MotionTrace {
    actor: string;
    fromNode: number;
    toNode: number;
    from: MotionPoint;
    to: MotionPoint;
    durationMs: number;
    done: boolean;
    /** Die Punkte des Fluges, in der Reihenfolge der Bilder. */
    samples: MotionSample[];
    /**
     * Der groesste Abstand der Bahn von der geraden Verbindung, als Anteil der
     * Sehnenlaenge. Gerechnet aus denselben Punkten, die gezeichnet wurden.
     */
    curvature: number;
}

/**
 * Der letzte Flug je Akteur.
 *
 * Er bleibt nach dem Ankommen stehen, damit der Beweislauf ihn NACH dem Flug
 * lesen kann. Waehrend eines Fluges eine halbe Sekunde lang alle 25 ms
 * nachzufragen hiesse, die Messung an der Antwortzeit einer Fernsteuerung
 * aufzuhaengen; hier steht, was das Bild getan hat.
 */
export const agentMotion: Record<string, MotionTrace> = {};

/** Hoechstens so viele Punkte je Flug werden aufgezeichnet. */
export const MOTION_SAMPLE_CAP = 160;

/** Wo die Kamera im zuletzt gezeichneten Bild stand. */
export const agentCamera: { position: MotionPoint; at: number } = {
    position: { x: 0, y: 0, z: 0 },
    at: 0,
};

/** Die Zeichenreihenfolgen der Szene, fuer die Messung "die Spur liegt darunter". */
export const agentRenderOrders: {
    trail: number;
    /** Die kleinste Zeichenreihenfolge aller anderen sichtbaren Objekte. */
    others: number;
    objects: number;
    /** Wie viele Spurlinien gerade in der Szene haengen. */
    trails: number;
    /** Strich und Luecke der Spur, in Welteinheiten. Null heisst: durchgezogen. */
    dash: [number, number];
} = { trail: TRAIL_RENDER_ORDER, others: 0, objects: 0, trails: 0, dash: [0, 0] };

/**
 * Wie weit zwei Bahnen um DENSELBEN Knoten mindestens auseinanderliegen.
 *
 * Zwei Akteure koennen am selben Symbol arbeiten, und dann liegen ihre Koerper
 * auf zwei Kreisen um denselben Punkt. Ohne einen Mindestabstand koennen sich
 * die beiden Punkte samt ihren Buchstaben in der Uebersicht ueberlagern; die
 * Lesbarkeitsmessung dieses Projekts zaehlt das als Ueberlagerung, und ein
 * Leser sieht dort einen Fleck statt zweier Wesen. Der Abstand ist in
 * Welteinheiten und gross genug, dass die Koerper auch in der weitesten
 * Uebersicht dieser Fixture getrennt bleiben.
 */
export const ORBIT_SEPARATION = 34;

/** Der feste Versatz eines Akteurs auf seiner Bahn, in Grad. */
export function phaseOf(id: string): number {
    return hashOf(id) % 360;
}

/** Der Winkel dieses Akteurs zu dieser Zeit, in Grad. */
export function angleAt(id: string, kind: WorkKind, elapsedMs: number): number {
    const orbit = ORBITS[kind];
    const turns = elapsedMs / orbit.periodMs;
    return (phaseOf(id) + turns * 360) % 360;
}

/**
 * Die Bahnradien aller Akteure, mit Mindestabstand am gemeinsamen Knoten.
 *
 * Die Art bestimmt weiter den Radius (lesen weit, schreiben eng); nur wenn zwei
 * Akteure denselben Knoten umkreisen, wird der zweite nach aussen geschoben, bis
 * der Abstand stimmt. Sortiert wird nach dem Radius der Art und dann nach der
 * Kennung, damit dieselbe Lage immer dieselben Bahnen ergibt: die Reihenfolge
 * des Eintreffens kommt darin nicht vor, also sieht dasselbe Bild nach einem
 * Reload genau so aus wie vorher.
 */
export function orbitRadii(actors: readonly ActorView[]): Map<string, number> {
    const out = new Map<string, number>();
    const groups = new Map<number, ActorView[]>();
    for (const actor of actors) {
        const id = actor.node?.id;
        if (id === undefined) {
            continue;
        }
        groups.set(id, [...(groups.get(id) ?? []), actor]);
    }
    for (const group of groups.values()) {
        const ordered = [...group].sort((a, b) =>
            ORBITS[a.kind].radius - ORBITS[b.kind].radius
            || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
        let last = Number.NEGATIVE_INFINITY;
        for (const actor of ordered) {
            const radius = Math.max(ORBITS[actor.kind].radius, last + ORBIT_SEPARATION);
            out.set(actor.id, radius);
            last = radius;
        }
    }
    return out;
}

/**
 * Die Lesung der Szene: wo steht die Kamera, und was ist die kleinste
 * Zeichenreihenfolge ausser der Spur?
 *
 * Beides wird gebraucht, um eine Behauptung zu einer Messung zu machen: dass
 * die Kamera einem Agenten weich folgt (AC3c) und dass die Spur unter den
 * echten Kanten liegt (AC2). Die Zeichenreihenfolgen werden nicht in jedem Bild
 * durchgezaehlt, sondern hoechstens einmal je Sekunde: eine Szene mit
 * fuenftausend Knoten hat einen Baum, und ihn sechzig Mal je Sekunde zu
 * durchlaufen waere ein Preis fuer eine Zahl, die sich nicht aendert.
 */
function SceneProbe(): null {
    const scanned = useRef(0);
    useFrame((state) => {
        agentCamera.position = {
            x: state.camera.position.x,
            y: state.camera.position.y,
            z: state.camera.position.z,
        };
        agentCamera.at = Date.now();
        const now = state.clock.getElapsedTime() * 1000;
        if (now - scanned.current < 1000) {
            return;
        }
        scanned.current = now;
        let others = Number.POSITIVE_INFINITY;
        let objects = 0;
        let trails = 0;
        let dash: [number, number] = [0, 0];
        state.scene.traverse((object) => {
            const drawable = object as THREE.Mesh;
            if (drawable.isMesh !== true && (object as THREE.Line).isLine !== true
                && (object as THREE.Points).isPoints !== true) {
                return;
            }
            objects += 1;
            if (object.name === 'atlas-agent-trail') {
                trails += 1;
                const material = (object as THREE.Line).material as THREE.LineDashedMaterial;
                dash = [material.dashSize ?? 0, material.gapSize ?? 0];
                return;
            }
            if (object.name === 'atlas-agent-tail') {
                return;
            }
            others = Math.min(others, object.renderOrder);
        });
        agentRenderOrders.others = Number.isFinite(others) ? others : 0;
        agentRenderOrders.objects = objects;
        agentRenderOrders.trails = trails;
        agentRenderOrders.dash = dash;
    }, -1);
    return null;
}

/**
 * Die Spur eines Akteurs: eine gestrichelte Linie durch die zuletzt besuchten
 * Knoten.
 *
 * Sie wird neu gebaut, wenn sich die Knotenliste aendert, und sonst nicht. Eine
 * Linie, die in jedem Bild ihre Punkte bekaeme, waere ein Puffer-Upload je Bild
 * fuer eine Geometrie, die zwischen zwei Ereignissen genau gleich bleibt.
 */
function AgentTrail(props: { actor: ActorView; limit: number }): JSX.Element | null {
    const { actor } = props;
    const nodes = actor.trail.slice(0, Math.max(0, props.limit));
    const key = nodes.map((node) => node.id).join(',');

    const line = useMemo(() => {
        const points = nodes.map((node) => new THREE.Vector3(node.x, node.y, node.z));
        const geometry = new THREE.BufferGeometry().setFromPoints(points);
        /*
         * Fein, gestrichelt, und dunkel genug, dass die Nachbearbeitung sie
         * nicht aufblendet.
         *
         * Der Bloom-Durchgang der Szene greift ab einer Leuchtdichte von 0.3
         * (GraphScene.tsx, luminanceThreshold). Bei halber Deckkraft liegt eine
         * Agentenfarbe darueber, und die Spur leuchtete dann heller als die
         * echten Kanten des Graphen: das Gegenteil der Zusicherung aus AC2. Bei
         * 0.3 bleibt sie unter der Schwelle, und der Unterschied zwischen "eine
         * Beziehung im Code" und "ein Weg, den jemand gegangen ist" bleibt am
         * Bild ablesbar.
         */
        const material = new THREE.LineDashedMaterial({
            color: new THREE.Color(actor.color),
            dashSize: 8,
            gapSize: 10,
            transparent: true,
            opacity: 0.3,
            depthWrite: false,
        });
        const object = new THREE.Line(geometry, material);
        object.name = 'atlas-agent-trail';
        object.renderOrder = TRAIL_RENDER_ORDER;
        object.computeLineDistances();
        return object;
        // Die Punkte haengen an der Liste der Knoten; `key` ist genau sie.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [key, actor.color]);

    useEffect(() => () => {
        line.geometry.dispose();
        (line.material as THREE.Material).dispose();
    }, [line]);

    if (nodes.length < 2) {
        return null;
    }
    return <primitive object={line} />;
}

/** Die Wellen eines Schreib-Bruchs. Genau eine je Bruch, nicht eine je Ereignis. */
function AgentWaves(props: { actor: ActorView }): JSX.Element | null {
    const { actor } = props;
    if (actor.waves.length === 0) {
        return null;
    }
    return (
        <>
            {actor.waves.map((burst) => {
                const node = actor.trail.find((entry) => entry.id === burst.nodeId)
                    ?? (actor.node?.id === burst.nodeId ? actor.node : undefined);
                if (node === undefined) {
                    return null;
                }
                return (
                    <Html
                        key={burst.key}
                        position={[node.x, node.y, node.z]}
                        center
                        style={{ pointerEvents: 'none' }}
                        zIndexRange={[60, 10]}
                    >
                        <span
                            className="atlas-agent-wave"
                            data-testid="atlas-agent-wave"
                            data-actor={actor.id}
                            data-node={burst.nodeId}
                            data-events={burst.events}
                            style={{ ['--atlas-agent-color' as string]: actor.color }}
                            aria-hidden="true"
                        />
                    </Html>
                );
            })}
        </>
    );
}

interface BodyEffects {
    tails: boolean;
    trails: boolean;
    waves: boolean;
}

function AgentBody(props: {
    actor: ActorView;
    radius: number;
    effects: BodyEffects;
    trailLimit: number;
}): JSX.Element | null {
    const { actor } = props;
    const group = useRef<THREE.Group | null>(null);
    const core = useRef<HTMLSpanElement | null>(null);
    const node = actor.node;

    /*
     * Die gestrichelte Verbindung zum geprueften Bereich.
     *
     * Als `THREE.Line` und nicht als JSX-Element, weil sie ihre Punkte in jedem
     * Bild neu bekommt: der Koerper wandert, das Ziel steht, und die Linie
     * zwischen beiden ist genau zwei Punkte, die sich aendern. Ein `useMemo` ist
     * die Stelle, an der genau ein Objekt daraus entsteht.
     */
    const line = useMemo(() => {
        const geometry = new THREE.BufferGeometry();
        geometry.setAttribute(
            'position',
            new THREE.BufferAttribute(new Float32Array(6), 3),
        );
        const material = new THREE.LineDashedMaterial({
            color: new THREE.Color(actor.color),
            dashSize: 6,
            gapSize: 6,
            transparent: true,
            opacity: 0.75,
            depthWrite: false,
        });
        return new THREE.Line(geometry, material);
    }, [actor.color]);

    /* Der Kometenschweif: die letzten Punkte des Fluges, als eine Linie. */
    const tail = useMemo(() => {
        const geometry = new THREE.BufferGeometry();
        geometry.setAttribute(
            'position',
            new THREE.BufferAttribute(new Float32Array(COMET_TAIL_POINTS * 3), 3),
        );
        geometry.setDrawRange(0, 0);
        const material = new THREE.LineBasicMaterial({
            color: new THREE.Color(actor.color),
            transparent: true,
            opacity: 0.85,
            depthWrite: false,
        });
        const object = new THREE.Line(geometry, material);
        object.name = 'atlas-agent-tail';
        object.frustumCulled = false;
        return object;
    }, [actor.color]);

    useEffect(() => () => {
        line.geometry.dispose();
        (line.material as THREE.Material).dispose();
        tail.geometry.dispose();
        (tail.material as THREE.Material).dispose();
    }, [line, tail]);

    /* Was zwischen zwei Bildern erinnert werden muss. */
    const seat = useRef<{
        nodeId: number;
        position: MotionPoint;
        flight: { from: MotionPoint; startedAt: number; sign: number } | null;
        landedAt: number;
        tailPoints: MotionPoint[];
        frozenElapsed: number | null;
        scale: number;
    }>({
        nodeId: -1,
        position: { x: 0, y: 0, z: 0 },
        flight: null,
        landedAt: -1,
        tailPoints: [],
        frozenElapsed: null,
        scale: 1,
    });

    /*
     * Der Takt. `-1` laesst diesen Rueckruf VOR dem von `Html` laufen, damit das
     * DOM-Element die Position dieses Bildes bekommt und nicht die des
     * vorigen. Eine negative Zahl uebernimmt den Renderloop nicht (das tut in
     * react-three-fiber erst eine positive), sie ordnet nur ein.
     */
    useFrame((state) => {
        if (node === undefined || group.current === null) {
            return;
        }
        const clockMs = state.clock.getElapsedTime() * 1000;
        const memory = seat.current;

        /*
         * Ruhe heisst Stillstand, und zwar an der Stelle, an der er
         * aufgehoert hat. Ihn auf seinen Ausgangswinkel zu setzen waere ein
         * Sprung, und ein Sprung ist eine Bewegung ohne Ereignis dahinter.
         */
        if (actor.idle) {
            memory.frozenElapsed = memory.frozenElapsed ?? clockMs;
        } else {
            memory.frozenElapsed = null;
        }
        const elapsed = memory.frozenElapsed ?? clockMs;

        const degrees = angleAt(actor.id, actor.kind, elapsed);
        agentAngles[actor.id] = degrees;
        const radians = (degrees * Math.PI) / 180;
        const orbit: MotionPoint = {
            x: node.x + Math.cos(radians) * props.radius,
            y: node.y + Math.sin(radians) * props.radius,
            z: node.z,
        };

        /* Ein neuer Ort ist ein Flug, und der erste Auftritt ist keiner. */
        if (memory.nodeId !== node.id) {
            if (memory.nodeId >= 0) {
                const from = { ...memory.position };
                memory.flight = {
                    from,
                    startedAt: clockMs,
                    sign: bendSignOf(hashOf(actor.id)),
                };
                memory.tailPoints = [from];
                agentMotion[actor.id] = {
                    actor: actor.id,
                    fromNode: memory.nodeId,
                    toNode: node.id,
                    from,
                    to: { ...orbit },
                    durationMs: TRANSITION_MS,
                    done: false,
                    /*
                     * Leer, weil der Punkt bei `t = 0` noch in DIESEM Bild
                     * dazukommt: der Flug faengt unten in derselben Runde an.
                     * Ihn hier schon einzutragen ergaebe zwei Punkte fuer
                     * denselben Augenblick, und eine Reihe mit zwei Zeiten null
                     * sieht aus wie ein Koerper, der an zwei Orten war.
                     */
                    samples: [],
                    curvature: 0,
                };
            }
            memory.nodeId = node.id;
        }

        let position = orbit;
        const flight = memory.flight;
        if (flight !== null) {
            const t = (clockMs - flight.startedAt) / TRANSITION_MS;
            const trace = agentMotion[actor.id];
            if (t >= 1) {
                memory.flight = null;
                memory.landedAt = clockMs;
                if (trace !== undefined && !trace.done) {
                    trace.to = { ...orbit };
                    trace.samples.push({ t: TRANSITION_MS, ...orbit });
                    trace.durationMs = clockMs - flight.startedAt;
                    trace.done = true;
                    const chord = Math.hypot(trace.to.x - trace.from.x, trace.to.y - trace.from.y);
                    trace.curvature = chord === 0
                        ? 0
                        : Number((Math.max(...trace.samples.map((sample) =>
                            distanceFromChord(sample, trace.from, trace.to))) / chord).toFixed(5));
                }
            } else {
                const control = controlPointOf(flight.from, orbit, flight.sign);
                position = transitionPointAt(flight.from, orbit, control, easeInOutCubic(t));
                if (trace !== undefined && trace.samples.length < MOTION_SAMPLE_CAP) {
                    trace.to = { ...orbit };
                    trace.samples.push({ t: clockMs - flight.startedAt, ...position });
                }
                memory.tailPoints.push(position);
                if (memory.tailPoints.length > COMET_TAIL_POINTS) {
                    memory.tailPoints.shift();
                }
            }
        }

        memory.position = position;
        agentPositions[actor.id] = position;
        group.current.position.set(position.x, position.y, position.z);

        /*
         * Der Schweif laeuft nach dem Ankommen noch kurz aus und ist danach weg.
         * Ein Schweif, der stehen bleibt, waere eine Bewegung, die schon vorbei
         * ist.
         */
        const fading = memory.flight === null
            && memory.landedAt >= 0
            && clockMs - memory.landedAt < COMET_TAIL_FADE_MS;
        if (memory.flight === null && !fading && memory.tailPoints.length > 0) {
            memory.tailPoints = [];
        }
        if (props.effects.tails) {
            const points = memory.tailPoints;
            const positions = tail.geometry.getAttribute('position') as THREE.BufferAttribute;
            for (let i = 0; i < points.length; i += 1) {
                const point = points[i] as MotionPoint;
                positions.setXYZ(i, point.x, point.y, point.z);
            }
            positions.needsUpdate = true;
            tail.geometry.setDrawRange(0, points.length >= 2 ? points.length : 0);
            (tail.material as THREE.LineBasicMaterial).opacity = fading
                ? 0.85 * (1 - (clockMs - memory.landedAt) / COMET_TAIL_FADE_MS)
                : 0.85;
            agentTails[actor.id] = points.length >= 2 ? points.length : 0;
        } else {
            tail.geometry.setDrawRange(0, 0);
            agentTails[actor.id] = 0;
        }

        /*
         * Der Puls. Er wird auf das DOM-Element geschrieben und nicht als
         * CSS-Animation gefahren, aus zwei Gruenden: der Takt haengt an einer
         * gezaehlten Zahl und aendert sich mit ihr, und ein Akteur ohne
         * Ereignisse bekommt so nachweisbar GAR KEINE Aenderung, statt einer
         * Animation mit Ausschlag null.
         */
        const scale = pulseScaleAt(actor.pulse, elapsed);
        if (core.current !== null && Math.abs(scale - memory.scale) > 0.001) {
            core.current.style.transform = scale === 1 ? '' : `scale(${scale.toFixed(3)})`;
            memory.scale = scale;
        }
        agentPulseScale[actor.id] = scale;

        const target = actor.testedNode;
        if (target !== undefined) {
            const positions = line.geometry.getAttribute('position') as THREE.BufferAttribute;
            positions.setXYZ(0, position.x, position.y, position.z);
            positions.setXYZ(1, target.x, target.y, target.z);
            positions.needsUpdate = true;
            line.geometry.computeBoundingSphere();
            line.computeLineDistances();
        }
    }, -1);

    if (node === undefined) {
        return null;
    }

    return (
        <>
            {props.effects.trails && <AgentTrail actor={actor} limit={props.trailLimit} />}
            {actor.testedNode !== undefined && <primitive object={line} />}
            {props.effects.tails && <primitive object={tail} />}
            {props.effects.waves && <AgentWaves actor={actor} />}
            <group ref={group} position={[node.x, node.y, node.z]}>
                <Html center style={{ pointerEvents: 'none' }} zIndexRange={[80, 20]}>
                    <span
                        className="atlas-agent"
                        data-testid="atlas-agent-body"
                        data-actor={actor.id}
                        data-kind={actor.kind}
                        data-letter={actor.letter}
                        data-you={actor.you}
                        data-idle={actor.idle}
                        data-color={actor.color}
                        data-pulse-ms={actor.pulse.periodMs}
                        data-pulse-events={actor.pulse.events}
                        title={text.pulseTitle(actor.pulse.events, actor.pulse.periodMs)}
                        style={{ ['--atlas-agent-color' as string]: actor.color }}
                    >
                        <span
                            className="atlas-agent-core"
                            data-testid="atlas-agent-core"
                            ref={core}
                            aria-hidden="true"
                        />
                        <span className="atlas-agent-letter" data-testid="atlas-agent-letter">
                            {actor.letter}
                        </span>
                    </span>
                </Html>
            </group>
            {!actor.idle && actor.ghostNodes.map((ghost) => (
                <Html
                    key={`${actor.id}-${ghost.id}`}
                    position={[ghost.x, ghost.y, ghost.z]}
                    center
                    style={{ pointerEvents: 'none' }}
                    zIndexRange={[70, 10]}
                >
                    <span
                        className="atlas-agent-ghost"
                        data-testid="atlas-agent-ghost"
                        data-actor={actor.id}
                        data-node={ghost.id}
                        style={{ ['--atlas-agent-color' as string]: actor.color }}
                        aria-hidden="true"
                    />
                </Html>
            ))}
        </>
    );
}

export interface AgentLayerProps {
    /** Die Akteure, die gezeichnet werden. Ohne Knoten wird nichts gezeichnet. */
    actors: readonly ActorView[];
    /**
     * Was von den teuren Wirkungen gezeichnet wird.
     *
     * Aus den Einstellungen (W11b AC7b), einzeln abschaltbar. Fehlt die Prop,
     * ist alles an: eine Ebene, die ohne Angabe weniger zeichnet, waere ein
     * stiller Sparmodus.
     */
    effects?: BodyEffects | undefined;
}

/**
 * Die Ebene. Sie zeichnet nur, was einen Knoten hat und was der Deckel
 * durchlaesst.
 *
 * Ein Akteur ohne Ort verschwindet damit vom Graphen und NICHT aus der Ansicht:
 * er steht im Instrument, unter denen, die sich nicht verorten lassen, samt
 * seinem Rohereignis. Ihn hier an eine Ersatzstelle zu setzen waere ein Koerper
 * an einem Knoten, an dem niemand arbeitet. Dasselbe gilt fuer den Deckel: wer
 * daran haengen bleibt, steht weiter im Instrument, und das Instrument sagt es.
 */
export function AgentLayer(props: AgentLayerProps): JSX.Element {
    const drawn = useMemo(
        () => props.actors.filter((actor) => actor.drawn),
        [props.actors],
    );
    const radii = useMemo(() => orbitRadii(drawn), [drawn]);
    const effects = props.effects ?? { tails: true, trails: true, waves: true };

    /*
     * Der Deckel der Spursegmente, von vorn vergeben.
     *
     * Die Akteure stehen in der Reihenfolge der juengsten Bewegung, also
     * bekommt der, der zuletzt gearbeitet hat, seine ganze Spur, und der Rest
     * so viel, wie uebrig ist. Nach dem Deckel bleibt der Koerper stehen und
     * nur die Spur wird kuerzer.
     */
    const limits = useMemo(() => {
        const out = new Map<string, number>();
        let left = TRAIL_SEGMENT_CAP;
        for (const actor of drawn) {
            const wanted = Math.max(0, actor.trail.length - 1);
            const given = Math.max(0, Math.min(wanted, left));
            out.set(actor.id, given + 1);
            left -= given;
        }
        return out;
    }, [drawn]);

    return (
        <group name="atlas-agent-layer">
            <SceneProbe />
            {drawn.map((actor) => (
                <AgentBody
                    key={actor.id}
                    actor={actor}
                    radius={radii.get(actor.id) ?? ORBITS[actor.kind].radius}
                    effects={effects}
                    trailLimit={limits.get(actor.id) ?? 0}
                />
            ))}
        </group>
    );
}

export default AgentLayer;
