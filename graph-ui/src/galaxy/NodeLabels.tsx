/*
 * MIT License. Copyright (c) 2025 DeusData.
 *
 * Uebernommen am 2026-08-28 aus DeusData/codebase-memory-mcp, Branch
 * feat/atlas-r1, Datei graph-ui/src/components/NodeLabels.tsx. Der Lizenztext
 * und die Liste aller uebernommenen Dateien stehen in THIRD_PARTY.md.
 *
 * Aenderungen gegenueber dem Original:
 *  - Importpfad: ../lib/types liegt hier neben der Datei.
 *  - Formatierung an den Stil dieses Projekts angeglichen (4 Leerzeichen,
 *    einfache Anfuehrungszeichen).
 *  - Logik unveraendert: Textur-Bau, Kuerzung, Sprite-Groesse und die Auswahl
 *    der bis zu 80 groessten Knoten sind die des Originals.
 *  - Neu (W5c): die drei Props `worldFontSize`, `maxTextWidth` und `onLayout`.
 *    Ohne sie verhaelt sich die Ebene wie das Original, also aendert sich die
 *    Galaxie nicht. Die Hierarchie-Ansicht setzt sie, und zwar aus einem
 *    Befund: dort haengt die Schriftgroesse eines Namens an der Knotengroesse,
 *    die aus dem Grad im GANZEN Graphen kommt, und zwei Nachbarn in einer
 *    Spalte konnten sich deshalb ueberlagern (Nutzerfeedback 2026-08-29,
 *    Screenshots). Eine feste Weltgroesse plus eine Textbreiten-Grenze macht
 *    die Beschriftung so gross wie das Raster sie erlaubt und nicht groesser.
 *    `onLayout` meldet die wirklich gezeichneten Kaesten nach oben, damit der
 *    Beweislauf sie messen kann, statt sie ein zweites Mal auszurechnen.
 *  - Neu (W10): die Prop `maxDistance`. Bis dahin haengt die Sichtbarkeit eines
 *    Namens REIN an der Knotengroesse (die groessten `maxLabels`), und die
 *    Kamera kommt darin nicht vor: ein Name aus der Tiefe des Bildes wird
 *    genauso gezeichnet wie einer direkt davor, mit einer eigenen Textur, einem
 *    eigenen Material und einem eigenen Zeichenaufruf. Mit `maxDistance` fallen
 *    die weit entfernten weg.
 *
 *    Sie werden AUSGEBLENDET und nicht ausgehaengt, und das ist der Grund, aus
 *    dem die Entscheidung in einem `useFrame` steht und nicht in React: die
 *    Entfernung aendert sich bei jeder Mausbewegung, und ein React-Durchgang je
 *    Bild waere teurer als die Sprites, die er einspart. `visible = false`
 *    ueberspringt den Zeichenaufruf, und die Textur bleibt liegen, damit ein
 *    Zurueckfahren der Kamera nicht achtzig Texturen neu baut.
 */

import { useCallback, useEffect, useMemo, useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import type { GraphNode } from './types';

/** Ein gezeichneter Namenskasten in Weltkoordinaten. */
export interface LabelBox {
    id: number;
    name: string;
    /** Mitte des Kastens. */
    x: number;
    y: number;
    width: number;
    height: number;
}

interface NodeLabelsProps {
    nodes: GraphNode[];
    highlightedIds: Set<number> | null;
    maxLabels?: number;
    /**
     * Feste Schriftgroesse in Weltmasse, statt der aus der Knotengroesse
     * abgeleiteten. Fehlt sie, rechnet die Ebene wie das Original.
     */
    worldFontSize?: number;
    /** Grenze der Textbreite in Texturpixeln. Fehlt sie, gilt die des Originals. */
    maxTextWidth?: number;
    /**
     * Entfernung zur Kamera, ab der ein Name nicht mehr gezeichnet wird.
     *
     * 0 oder fehlend heisst: keine Grenze, also das Verhalten der Uebernahme.
     */
    maxDistance?: number;
    /** Was wirklich gezeichnet wurde, in Weltkoordinaten. */
    onLayout?: ((boxes: LabelBox[]) => void) | undefined;
}

interface LabelTexture {
    texture: THREE.CanvasTexture;
    width: number;
    height: number;
}

const TEXTURE_FONT_SIZE = 64;
const TEXTURE_FONT =
    `600 ${TEXTURE_FONT_SIZE}px Inter, system-ui, -apple-system, ` +
    'BlinkMacSystemFont, "Segoe UI", sans-serif';
const TEXTURE_MAX_TEXT_WIDTH = 720;
const TEXTURE_PADDING_X = 24;
const TEXTURE_PADDING_Y = 14;
const TEXTURE_STROKE_WIDTH = 8;

function fitText(
    ctx: CanvasRenderingContext2D,
    text: string,
    maxWidth: number,
): string {
    if (ctx.measureText(text).width <= maxWidth) return text;

    let lo = 0;
    let hi = text.length;
    while (lo < hi) {
        const mid = Math.ceil((lo + hi) / 2);
        const candidate = `${text.slice(0, mid)}...`;
        if (ctx.measureText(candidate).width <= maxWidth) lo = mid;
        else hi = mid - 1;
    }

    return `${text.slice(0, Math.max(1, lo))}...`;
}

function createLabelTexture(
    name: string,
    color: string,
    maxTextWidth = TEXTURE_MAX_TEXT_WIDTH,
): LabelTexture | null {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    if (!ctx) {
        return null;
    }

    ctx.font = TEXTURE_FONT;
    const text = fitText(ctx, name, maxTextWidth);
    const textWidth = Math.ceil(ctx.measureText(text).width);
    const logicalWidth = Math.max(
        1,
        textWidth + TEXTURE_PADDING_X * 2 + TEXTURE_STROKE_WIDTH * 2,
    );
    const logicalHeight =
        TEXTURE_FONT_SIZE + TEXTURE_PADDING_Y * 2 + TEXTURE_STROKE_WIDTH * 2;
    const pixelRatio =
        typeof window === 'undefined'
            ? 1
            : Math.min(window.devicePixelRatio || 1, 2);

    canvas.width = Math.ceil(logicalWidth * pixelRatio);
    canvas.height = Math.ceil(logicalHeight * pixelRatio);
    canvas.style.width = `${logicalWidth}px`;
    canvas.style.height = `${logicalHeight}px`;

    ctx.scale(pixelRatio, pixelRatio);
    ctx.font = TEXTURE_FONT;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.lineJoin = 'round';
    ctx.lineWidth = TEXTURE_STROKE_WIDTH;
    ctx.strokeStyle = 'rgba(0, 0, 0, 0.9)';
    ctx.fillStyle = color;

    const x = logicalWidth / 2;
    const y = logicalHeight / 2;
    ctx.strokeText(text, x, y);
    ctx.fillText(text, x, y);

    const texture = new THREE.CanvasTexture(canvas);
    texture.colorSpace = THREE.SRGBColorSpace;
    texture.minFilter = THREE.LinearFilter;
    texture.magFilter = THREE.LinearFilter;
    texture.generateMipmaps = false;
    texture.needsUpdate = true;

    return {
        texture,
        width: logicalWidth,
        height: logicalHeight,
    };
}

function NodeLabelSprite({
    node,
    worldFontSize: fixedFontSize,
    maxTextWidth,
    onMeasure,
    spriteRef,
}: {
    node: GraphNode;
    worldFontSize?: number | undefined;
    maxTextWidth?: number | undefined;
    onMeasure?: ((box: LabelBox | undefined) => void) | undefined;
    /** Der gezeichnete Sprite, damit die Entfernungspruefung ihn erreicht. */
    spriteRef?: ((sprite: THREE.Sprite | null) => void) | undefined;
}) {
    const label = useMemo(
        () => createLabelTexture(node.name, node.color, maxTextWidth),
        [node.name, node.color, maxTextWidth],
    );

    useEffect(() => {
        return () => label?.texture.dispose();
    }, [label]);

    const worldFontSize = fixedFontSize ?? Math.max(1.8, node.size * 0.4);
    const worldHeight = label === null ? 0 : worldFontSize * (label.height / TEXTURE_FONT_SIZE);
    const worldWidth = label === null ? 0 : worldHeight * (label.width / label.height);
    const centreY = node.y + node.size * 0.7 + worldHeight / 2;

    /* Der gezeichnete Kasten, gemeldet statt anderswo nachgerechnet. */
    useEffect(() => {
        if (onMeasure === undefined) {
            return;
        }
        if (label === null) {
            onMeasure(undefined);
            return;
        }
        onMeasure({
            id: node.id,
            name: node.name,
            x: node.x,
            y: centreY,
            width: worldWidth,
            height: worldHeight,
        });
        return () => onMeasure(undefined);
    }, [onMeasure, label, node.id, node.name, node.x, centreY, worldWidth, worldHeight]);

    if (!label) return null;

    return (
        <sprite
            ref={spriteRef}
            position={[node.x, centreY, node.z]}
            scale={[worldWidth, worldHeight, 1]}
            renderOrder={20}
            frustumCulled={false}
        >
            <spriteMaterial
                map={label.texture}
                transparent
                depthWrite={false}
                toneMapped={false}
            />
        </sprite>
    );
}

export function NodeLabels({
    nodes,
    highlightedIds,
    maxLabels = 80,
    worldFontSize,
    maxTextWidth,
    maxDistance = 0,
    onLayout,
}: NodeLabelsProps) {
    const labeled = useMemo(() => {
        const hasHighlight = highlightedIds && highlightedIds.size > 0;

        if (hasHighlight) {
            return nodes
                .filter((n) => highlightedIds.has(n.id))
                .sort((a, b) => b.size - a.size)
                .slice(0, maxLabels);
        }

        return [...nodes].sort((a, b) => b.size - a.size).slice(0, maxLabels);
    }, [nodes, highlightedIds, maxLabels]);

    /* Die Kaesten bleiben fuer die Lebensdauer ihrer Sprites erhalten. Erst
     * deren aktuelle Sichtbarkeit entscheidet, ob der Kasten wirklich
     * gezeichnet wird und folglich nach oben gemeldet werden darf. */
    const boxes = useRef(new Map<number, LabelBox>());
    const measure = useCallback((box: LabelBox | undefined, id: number) => {
        if (box === undefined) {
            if (boxes.current.delete(id)) {
                visibleSignature.current = undefined;
            }
            return;
        }
        if (!boxes.current.has(id)) {
            visibleSignature.current = undefined;
        }
        boxes.current.set(id, box);
    }, []);

    /*
     * Die Entfernungspruefung (W10), einmal je Bild fuer alle Namen.
     *
     * Hoechstens `maxLabels` Sprites, also achtzig Abstandsrechnungen: das ist
     * billiger als ein einziger zusaetzlicher Zeichenaufruf mit Textur. Ohne
     * Grenze wird nichts gerechnet und nichts angefasst, damit die Vorgabe
     * genau das Verhalten der Uebernahme bleibt.
     */
    const sprites = useRef(new Map<number, THREE.Sprite>());
    const registerSprite = useCallback((sprite: THREE.Sprite | null, id: number) => {
        if (sprite === null) {
            if (sprites.current.delete(id)) {
                visibleSignature.current = undefined;
            }
            return;
        }
        if (!sprites.current.has(id)) {
            visibleSignature.current = undefined;
        }
        sprites.current.set(id, sprite);
    }, []);

    const layoutCallback = useRef(onLayout);
    const visibleSignature = useRef<string | undefined>(undefined);
    useEffect(() => {
        layoutCallback.current = onLayout;
        /* Ein neuer Empfaenger soll die aktuelle Lage einmal erhalten, auch
         * wenn die sichtbaren IDs seit seinem Vorgaenger gleich blieben. */
        visibleSignature.current = undefined;
    }, [onLayout]);

    const publishVisible = useCallback(() => {
        const visible = [...boxes.current.values()]
            .filter((box) => sprites.current.get(box.id)?.visible === true)
            .sort((left, right) => left.id - right.id);
        const signature = visible.map((box) => box.id).join(',');
        if (signature === visibleSignature.current) {
            return;
        }
        visibleSignature.current = signature;
        layoutCallback.current?.(visible);
    }, []);

    useFrame((state) => {
        if (!(maxDistance > 0)) {
            publishVisible();
            return;
        }
        let changed = visibleSignature.current === undefined;
        for (const sprite of sprites.current.values()) {
            const visible = state.camera.position.distanceTo(sprite.position) <= maxDistance;
            if (sprite.visible !== visible) {
                sprite.visible = visible;
                changed = true;
            }
        }
        if (changed) {
            publishVisible();
        }
    });

    // Wird die Grenze weggenommen, muss das Ausgeblendete zurueckkommen: das
    // useFrame oben fasst dann nichts mehr an, und was unsichtbar war, bliebe
    // es fuer immer.
    useEffect(() => {
        if (maxDistance > 0) {
            return;
        }
        let changed = visibleSignature.current === undefined;
        for (const sprite of sprites.current.values()) {
            if (!sprite.visible) {
                sprite.visible = true;
                changed = true;
            }
        }
        if (changed) {
            publishVisible();
        }
    }, [maxDistance, labeled, publishVisible]);

    useEffect(() => () => {
        boxes.current.clear();
        sprites.current.clear();
        if (visibleSignature.current !== '') {
            visibleSignature.current = '';
            layoutCallback.current?.([]);
        }
    }, []);

    return (
        <group>
            {labeled.map((node) => (
                <NodeLabelSprite
                    key={node.id}
                    node={node}
                    worldFontSize={worldFontSize}
                    maxTextWidth={maxTextWidth}
                    onMeasure={(box) => measure(box, node.id)}
                    spriteRef={(sprite) => registerSprite(sprite, node.id)}
                />
            ))}
        </group>
    );
}
