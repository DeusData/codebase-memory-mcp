/**
 * Wie weit die Kamera stehen muss, damit ein Rechteck ganz ins Bild passt.
 *
 * Reine Trigonometrie, ohne three und ohne DOM, damit die eine Zahl, an der die
 * Lesbarkeit der Hierarchie haengt, ohne Browser pruefbar ist.
 *
 * **Warum ueberhaupt eine eigene Rechnung.** `computeCameraTarget` der
 * Uebernahme rahmt eine Punktwolke: es nimmt den Schwerpunkt, misst den
 * groessten Abstand dazu und multipliziert ihn mit drei, mit einer Untergrenze
 * von 300 Einheiten. Fuer eine Wolke aus tausend Sternen ist das richtig. Fuer
 * einen Walk aus vier Symbolen ist es das nicht: der groesste Abstand ist dort
 * klein, die Untergrenze greift, und die vier Punkte stehen als Briefmarke in
 * der Mitte eines leeren Panels (Nutzerfeedback 2026-08-29, Screenshots). Diese
 * Datei rechnet stattdessen aus dem Oeffnungswinkel und dem Seitenverhaeltnis
 * die Entfernung, bei der das Rechteck genau hineinpasst, und legt einen Rand
 * darum.
 *
 * **Warum frontal.** Die Uebernahme stellt die Kamera schraeg ueber die Wolke,
 * und in drei Dimensionen ist das die bessere Ansicht. Die Hierarchie ist eine
 * flache Zeichnung in der Ebene z=0; schraeg darauf zu sehen hiesse, ein Raster
 * aus Spalten und Zeilen perspektivisch zu verziehen, damit es raeumlich
 * aussieht. Also steht die Kamera frontal davor.
 *
 * **Seit W10 gibt es hier zwei Rahmungen.** {@link frameDistance} rahmt fuer die
 * perspektivische Kamera, {@link orthographicZoom} fuer die flache Ansicht des
 * Einstellungen-Panels. Sie stehen nebeneinander und nicht ineinander, weil die
 * beiden Kameras nach verschiedenen Groessen fragen: die eine nach einer
 * Entfernung, die andere nach Pixeln je Welteinheit. Eine gemeinsame Funktion
 * haette einen Parameter, der entscheidet, welche Haelfte gilt.
 *
 * **Seit W10b kommt die Ausrichtung dazu.** Beide Rahmungen oben setzen voraus,
 * dass die Kamera schon in die richtige Richtung sieht: sie rechnen eine
 * Entfernung fuer ein Rechteck in der Ebene z=0. Das Layout des Servers ist
 * aber dreidimensional und in den drei Richtungen verschieden weit, und eine
 * Kamera, die die duenne Richtung entlangsieht, zeigt von einer Wolke einen
 * Strich (Nutzerbefund 2026-08-30: "mich nervt, dass die Galaxy manchmal
 * einfach flach ist"). {@link principalFrame} rechnet darum aus den Positionen
 * selbst, wo die groesste Flaeche der Wolke liegt, und {@link fitCamera} stellt
 * die Kamera senkrecht davor, weit genug weg, dass alles hineinpasst.
 */

/** Ein achsenparalleles Rechteck in Weltkoordinaten. */
export interface FrameBox {
    centerX: number;
    centerY: number;
    width: number;
    height: number;
}

/**
 * Der Rand um das Gerahmte.
 *
 * Zwoelf Prozent: genug, dass die aeussersten Beschriftungen nicht am Rand
 * kleben, und wenig genug, dass ein kleiner Walk das Panel wirklich fuellt.
 */
export const FRAME_MARGIN = 1.12;

/** Unter diese Entfernung faehrt die Kamera nie, egal wie klein das Bild ist. */
export const FRAME_MIN_DISTANCE = 40;

/**
 * Die Entfernung, bei der `box` vollstaendig im Bild steht.
 *
 * `fovDegrees` ist der SENKRECHTE Oeffnungswinkel, so wie three ihn fuehrt, und
 * `aspect` ist Breite durch Hoehe der Zeichenflaeche. Die waagerechte Bedingung
 * teilt darum durch das Seitenverhaeltnis: ein breites Panel braucht fuer
 * dieselbe Breite weniger Abstand als ein schmales.
 *
 * Total: ein Rechteck ohne Ausdehnung, ein Seitenverhaeltnis von null und ein
 * unsinniger Winkel ergeben die Untergrenze statt NaN.
 */
export function frameDistance(
    box: FrameBox,
    fovDegrees: number,
    aspect: number,
    margin: number = FRAME_MARGIN,
): number {
    const fov = (Math.max(1, Math.min(179, fovDegrees)) * Math.PI) / 180;
    const half = Math.tan(fov / 2);
    const safeAspect = Number.isFinite(aspect) && aspect > 0 ? aspect : 1;
    const scale = Number.isFinite(margin) && margin > 0 ? margin : 1;
    const height = Math.max(0, box.height) * scale;
    const width = Math.max(0, box.width) * scale;
    const vertical = height / 2 / half;
    const horizontal = width / 2 / (half * safeAspect);
    const distance = Math.max(vertical, horizontal);
    return Number.isFinite(distance) ? Math.max(FRAME_MIN_DISTANCE, distance) : FRAME_MIN_DISTANCE;
}

/**
 * Wie viel des Bildes das Gerahmte danach einnimmt, in beiden Richtungen.
 *
 * Der Beweislauf liest daran, dass ein kleiner Walk das Panel wirklich fuellt,
 * statt als Briefmarke darin zu stehen. Ein Wert von 1 heisst "genau
 * formatfuellend", darunter bleibt Rand.
 */
/**
 * Das umschliessende Rechteck einer Punktmenge in der Ebene.
 *
 * Nur x und y: die Rahmung der flachen Ansicht laesst z fallen, und ein
 * Rechteck, das eine dritte Achse mittelte, waere ein Rechteck ueber etwas
 * anderem als dem, was zu sehen ist. Ohne Punkte ergibt sich ein Rechteck ohne
 * Ausdehnung, und die Rechnungen darunter fangen den Fall selbst ab.
 */
export function flatBounds(points: readonly { x: number; y: number }[]): FrameBox {
    if (points.length === 0) {
        return { centerX: 0, centerY: 0, width: 0, height: 0 };
    }
    let minX = Number.POSITIVE_INFINITY;
    let maxX = Number.NEGATIVE_INFINITY;
    let minY = Number.POSITIVE_INFINITY;
    let maxY = Number.NEGATIVE_INFINITY;
    for (const point of points) {
        if (!Number.isFinite(point.x) || !Number.isFinite(point.y)) {
            continue;
        }
        minX = Math.min(minX, point.x);
        maxX = Math.max(maxX, point.x);
        minY = Math.min(minY, point.y);
        maxY = Math.max(maxY, point.y);
    }
    if (!Number.isFinite(minX) || !Number.isFinite(minY)) {
        return { centerX: 0, centerY: 0, width: 0, height: 0 };
    }
    return {
        centerX: (minX + maxX) / 2,
        centerY: (minY + maxY) / 2,
        width: maxX - minX,
        height: maxY - minY,
    };
}

/**
 * Der Zoom, mit dem eine orthografische Kamera dieses Rechteck ganz zeigt.
 *
 * Hier steht ausdruecklich KEINE Trigonometrie, und das ist der Unterschied
 * zwischen den beiden Kameras: eine perspektivische braucht den
 * Oeffnungswinkel, weil ihr Ausschnitt mit der Entfernung waechst. Eine
 * orthografische hat keinen Ausschnittkegel; three rechnet ihre Ebenen aus der
 * Groesse der Zeichenflaeche und teilt durch `zoom`. Der Ausschnitt ergibt sich
 * damit direkt aus Pixeln je Welteinheit, und die Entfernung der Kamera aendert
 * daran nichts.
 *
 * Genommen wird die kleinere der beiden Richtungen, damit das Rechteck in beide
 * hineinpasst. Total: ein Rechteck ohne Ausdehnung und eine Zeichenflaeche ohne
 * Pixel ergeben {@link FLAT_MIN_ZOOM} statt einer Division durch null.
 */
export const FLAT_MIN_ZOOM = 0.01;

export function orthographicZoom(
    box: FrameBox,
    pixelWidth: number,
    pixelHeight: number,
    margin: number = FRAME_MARGIN,
): number {
    const scale = Number.isFinite(margin) && margin > 0 ? margin : 1;
    const width = Math.max(0, box.width) * scale;
    const height = Math.max(0, box.height) * scale;
    if (!(pixelWidth > 0) || !(pixelHeight > 0) || width <= 0 || height <= 0) {
        return FLAT_MIN_ZOOM;
    }
    const zoom = Math.min(pixelWidth / width, pixelHeight / height);
    return Number.isFinite(zoom) && zoom > FLAT_MIN_ZOOM ? zoom : FLAT_MIN_ZOOM;
}

export function frameFill(
    box: FrameBox,
    distance: number,
    fovDegrees: number,
    aspect: number,
): { vertical: number; horizontal: number } {
    const fov = (Math.max(1, Math.min(179, fovDegrees)) * Math.PI) / 180;
    const safeAspect = Number.isFinite(aspect) && aspect > 0 ? aspect : 1;
    const visibleHeight = 2 * Math.max(FRAME_MIN_DISTANCE, distance) * Math.tan(fov / 2);
    const visibleWidth = visibleHeight * safeAspect;
    return {
        vertical: visibleHeight === 0 ? 0 : Math.max(0, box.height) / visibleHeight,
        horizontal: visibleWidth === 0 ? 0 : Math.max(0, box.width) / visibleWidth,
    };
}

/* ------------------------------------------- die Ausrichtung (W10b, AC5) --- */

/** Ein Punkt im Raum. Absichtlich ohne three: diese Datei rechnet ohne Szene. */
export interface Vector3Like {
    x: number;
    y: number;
    z: number;
}

/**
 * Die drei Hauptachsen einer Punktwolke, nach ihrer Ausdehnung geordnet.
 *
 * `axes[0]` ist die weiteste Richtung, `axes[2]` die duennste; zusammen mit
 * `extents` ist das die Antwort auf die Frage, wo die groesste Flaeche der Wolke
 * liegt. Das Dreibein ist rechtshaendig (`axes[2]` ist das Kreuzprodukt der
 * beiden anderen), weil die Kamera es genau so als Basis benutzt: Blick entlang
 * `axes[2]`, `axes[1]` nach oben, `axes[0]` nach rechts.
 */
export interface PrincipalFrame {
    /** Der Mittelpunkt des umschliessenden Kastens IN diesen Achsen. */
    center: Vector3Like;
    axes: [Vector3Like, Vector3Like, Vector3Like];
    /** Ausdehnung entlang jeder Achse, in derselben Reihenfolge. */
    extents: [number, number, number];
    /** Wie viele Punkte wirklich in die Rechnung eingegangen sind. */
    counted: number;
}

const ZERO: Vector3Like = { x: 0, y: 0, z: 0 };
const IDENTITY: [Vector3Like, Vector3Like, Vector3Like] = [
    { x: 1, y: 0, z: 0 },
    { x: 0, y: 1, z: 0 },
    { x: 0, y: 0, z: 1 },
];

const dot = (a: Vector3Like, b: Vector3Like): number => a.x * b.x + a.y * b.y + a.z * b.z;

const cross = (a: Vector3Like, b: Vector3Like): Vector3Like => ({
    x: a.y * b.z - a.z * b.y,
    y: a.z * b.x - a.x * b.z,
    z: a.x * b.y - a.y * b.x,
});

const norm = (a: Vector3Like): Vector3Like => {
    const length = Math.sqrt(dot(a, a));
    return length > 0 ? { x: a.x / length, y: a.y / length, z: a.z / length } : { x: 0, y: 0, z: 1 };
};

/**
 * Das Vorzeichen einer Achse festnageln.
 *
 * Ein Eigenvektor ist nur bis auf sein Vorzeichen bestimmt, und ohne diese Regel
 * stuende die Kamera bei derselben Wolke mal vor und mal hinter ihr, je nachdem
 * wie die Rechnung gerade ausgeht. Genommen wird die groesste Komponente; ist
 * sie negativ, wird die ganze Achse umgedreht. Das ist willkuerlich, aber
 * WIEDERHOLBAR, und darauf kommt es an.
 */
function orient(a: Vector3Like): Vector3Like {
    const entries: [number, number][] = [[Math.abs(a.x), a.x], [Math.abs(a.y), a.y], [Math.abs(a.z), a.z]];
    let best = 0;
    for (let i = 1; i < entries.length; i += 1) {
        if ((entries[i] as [number, number])[0] > (entries[best] as [number, number])[0] + 1e-12) {
            best = i;
        }
    }
    return (entries[best] as [number, number])[1] < 0 ? { x: -a.x, y: -a.y, z: -a.z } : a;
}

/**
 * Eigenwerte und Eigenvektoren einer symmetrischen 3x3-Matrix, nach Jacobi.
 *
 * Zyklisch und mit fester Rundenzahl, also ohne Abbruch nach Gefuehl: dieselbe
 * Eingabe gibt dieselbe Ausgabe, und mehr braucht diese Datei nicht. Die Matrix
 * ist eine Kovarianz, also symmetrisch und reell, und damit ist Jacobi der
 * kuerzeste Weg, der ohne Bibliothek auskommt.
 */
function jacobiEigen(input: number[][]): { values: number[]; vectors: number[][] } {
    const a = input.map((row) => [...row]);
    const v = [[1, 0, 0], [0, 1, 0], [0, 0, 1]];
    for (let sweep = 0; sweep < 24; sweep += 1) {
        let off = 0;
        for (const [p, q] of [[0, 1], [0, 2], [1, 2]] as [number, number][]) {
            off += Math.abs((a[p] as number[])[q] as number);
        }
        if (off < 1e-12) {
            break;
        }
        for (const [p, q] of [[0, 1], [0, 2], [1, 2]] as [number, number][]) {
            const apq = (a[p] as number[])[q] as number;
            if (Math.abs(apq) < 1e-15) {
                continue;
            }
            const app = (a[p] as number[])[p] as number;
            const aqq = (a[q] as number[])[q] as number;
            const theta = (aqq - app) / (2 * apq);
            const t = Math.sign(theta || 1) / (Math.abs(theta) + Math.sqrt(theta * theta + 1));
            const c = 1 / Math.sqrt(t * t + 1);
            const s = t * c;
            for (let k = 0; k < 3; k += 1) {
                const akp = (a[k] as number[])[p] as number;
                const akq = (a[k] as number[])[q] as number;
                (a[k] as number[])[p] = c * akp - s * akq;
                (a[k] as number[])[q] = s * akp + c * akq;
            }
            for (let k = 0; k < 3; k += 1) {
                const apk = (a[p] as number[])[k] as number;
                const aqk = (a[q] as number[])[k] as number;
                (a[p] as number[])[k] = c * apk - s * aqk;
                (a[q] as number[])[k] = s * apk + c * aqk;
            }
            for (let k = 0; k < 3; k += 1) {
                const vkp = (v[k] as number[])[p] as number;
                const vkq = (v[k] as number[])[q] as number;
                (v[k] as number[])[p] = c * vkp - s * vkq;
                (v[k] as number[])[q] = s * vkp + c * vkq;
            }
        }
    }
    return {
        values: [(a[0] as number[])[0] as number, (a[1] as number[])[1] as number, (a[2] as number[])[2] as number],
        vectors: v,
    };
}

/**
 * Wo die groesste Flaeche einer Punktwolke liegt.
 *
 * Die Achsen kommen aus der Kovarianz der Positionen (also aus der Streuung),
 * die Ausdehnungen aus dem umschliessenden Kasten IN diesen Achsen (also aus
 * den aeussersten Punkten). Beides zusammen und nicht eines von beiden allein:
 * die Streuung sagt, WIE die Wolke liegt, und die aeussersten Punkte sagen, wie
 * viel Bild sie braucht. Sortiert wird nach der Ausdehnung und nicht nach dem
 * Eigenwert, denn gerahmt wird der Kasten und nicht die Varianz.
 *
 * Total: keine Punkte, ein Punkt oder lauter unbrauchbare Zahlen ergeben die
 * Weltachsen und ein Dreibein ohne Ausdehnung. Ein Sonderfall, der still ein
 * NaN in die Kamera schreibt, waere ein schwarzes Bild ohne Meldung.
 */
export function principalFrame(points: readonly Vector3Like[]): PrincipalFrame {
    const usable: Vector3Like[] = [];
    for (const point of points) {
        if (Number.isFinite(point.x) && Number.isFinite(point.y) && Number.isFinite(point.z)) {
            usable.push(point);
        }
    }
    if (usable.length === 0) {
        return { center: ZERO, axes: IDENTITY, extents: [0, 0, 0], counted: 0 };
    }

    let mx = 0;
    let my = 0;
    let mz = 0;
    for (const point of usable) {
        mx += point.x;
        my += point.y;
        mz += point.z;
    }
    mx /= usable.length;
    my /= usable.length;
    mz /= usable.length;

    const covariance = [[0, 0, 0], [0, 0, 0], [0, 0, 0]];
    for (const point of usable) {
        const d = [point.x - mx, point.y - my, point.z - mz];
        for (let i = 0; i < 3; i += 1) {
            for (let j = 0; j < 3; j += 1) {
                (covariance[i] as number[])[j] =
                    ((covariance[i] as number[])[j] as number) + (d[i] as number) * (d[j] as number);
            }
        }
    }
    for (let i = 0; i < 3; i += 1) {
        for (let j = 0; j < 3; j += 1) {
            (covariance[i] as number[])[j] = ((covariance[i] as number[])[j] as number) / usable.length;
        }
    }

    const { vectors } = jacobiEigen(covariance);
    const candidates: Vector3Like[] = [0, 1, 2].map((column) => orient(norm({
        x: (vectors[0] as number[])[column] as number,
        y: (vectors[1] as number[])[column] as number,
        z: (vectors[2] as number[])[column] as number,
    })));

    /*
     * Gemessen wird die Ausdehnung, danach wird sortiert.
     *
     * Der Eigenwert waere die Varianz, und die kann bei einer Wolke mit einem
     * dichten Kern und wenigen Ausreissern eine andere Reihenfolge ergeben als
     * der Kasten. Ins Bild muss der Kasten.
     */
    const measured = candidates.map((axis) => {
        let min = Number.POSITIVE_INFINITY;
        let max = Number.NEGATIVE_INFINITY;
        for (const point of usable) {
            const value = dot(point, axis);
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return { axis, min, max, extent: max - min, middle: (min + max) / 2 };
    });
    measured.sort((a, b) => b.extent - a.extent);

    const first = orient((measured[0] as { axis: Vector3Like }).axis);
    const second = orient((measured[1] as { axis: Vector3Like }).axis);
    // Rechtshaendig, damit die dritte Achse wirklich die Blickrichtung ist und
    // das Bild nicht spiegelverkehrt steht.
    const third = norm(cross(first, second));

    const along = (axis: Vector3Like): { extent: number; middle: number } => {
        let min = Number.POSITIVE_INFINITY;
        let max = Number.NEGATIVE_INFINITY;
        for (const point of usable) {
            const value = dot(point, axis);
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return { extent: max - min, middle: (min + max) / 2 };
    };
    const a0 = along(first);
    const a1 = along(second);
    const a2 = along(third);

    return {
        center: {
            x: first.x * a0.middle + second.x * a1.middle + third.x * a2.middle,
            y: first.y * a0.middle + second.y * a1.middle + third.y * a2.middle,
            z: first.z * a0.middle + second.z * a1.middle + third.z * a2.middle,
        },
        axes: [first, second, third],
        extents: [a0.extent, a1.extent, a2.extent],
        counted: usable.length,
    };
}

/** Wo die Kamera steht, damit die ganze Wolke im Bild ist. */
export interface CameraFit {
    /** Der Punkt, auf den die Kamera sieht. */
    center: Vector3Like;
    /** Wo sie dafuer steht. */
    eye: Vector3Like;
    /** Ihre Oben-Richtung: die zweitweiteste Achse der Wolke. */
    up: Vector3Like;
    /** Die Richtung, aus der sie sieht: die duennste Achse der Wolke. */
    normal: Vector3Like;
    /** Die Ausdehnungen der Wolke in genau diesen drei Richtungen. */
    width: number;
    height: number;
    depth: number;
    /** Wie weit die Kamera vom Mittelpunkt weg steht. */
    distance: number;
    counted: number;
}

/**
 * Die Kamera senkrecht auf die groesste Flaeche stellen, mit Rand.
 *
 * Zwei Zutaten, und beide sind Nutzerbefunde vom 2026-08-30:
 *
 *  1. **Die Richtung.** Sie kommt aus {@link principalFrame}: die Kamera sieht
 *     die duennste Richtung entlang, also auf die groesste Flaeche. Sonst zeigt
 *     eine flache Wolke ihre Kante.
 *  2. **Der Abstand.** Er rahmt das Rechteck aus den beiden weiten Achsen UND
 *     legt die halbe Tiefe drauf. Ohne diesen Zuschlag waere die Rahmung fuer
 *     die Ebene durch den Mittelpunkt richtig und fuer alles, was naeher an der
 *     Kamera liegt, zu eng: ein Punkt auf der vorderen Haelfte steht naeher und
 *     erscheint damit weiter aussen, als das Rechteck erlaubt. Mit dem Zuschlag
 *     liegt die vordere Ebene der Wolke genau auf der gerahmten Entfernung, und
 *     jeder Punkt dahinter ist nur noch kleiner.
 *
 * Der Rand ist derselbe wie bei jeder anderen Rahmung dieser Datei
 * ({@link FRAME_MARGIN}), damit "eingepasst" ueberall dasselbe heisst.
 */
export function fitCamera(
    points: readonly Vector3Like[],
    fovDegrees: number,
    aspect: number,
    margin: number = FRAME_MARGIN,
): CameraFit | null {
    const frame = principalFrame(points);
    if (frame.counted === 0) {
        return null;
    }
    const [width, height, depth] = frame.extents;
    const planar = frameDistance(
        { centerX: 0, centerY: 0, width, height },
        fovDegrees,
        aspect,
        margin,
    );
    const distance = planar + depth / 2;
    const normal = (frame.axes[2] as Vector3Like);
    return {
        center: frame.center,
        eye: {
            x: frame.center.x + normal.x * distance,
            y: frame.center.y + normal.y * distance,
            z: frame.center.z + normal.z * distance,
        },
        up: frame.axes[1] as Vector3Like,
        normal,
        width,
        height,
        depth,
        distance,
        counted: frame.counted,
    };
}
