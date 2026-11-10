/**
 * Die beiden Hashes, die die portierte Verstehens-Logik braucht, ohne Node.
 *
 * Im Referenzprojekt kommen beide aus `node:crypto`: der IR-Builder hasht den
 * Quelltext mit sha256, damit eine zwischengespeicherte IR von einer
 * Bearbeitung und nicht von einer Uhr ungueltig wird, und der Checklisten-
 * Generator leitet die Id eines Punktes mit sha1 aus Kategorie und Ziel ab,
 * damit ein Haken eine Neuindizierung ueberlebt. Beides laeuft hier im
 * Browser, wo es `node:crypto` nicht gibt.
 *
 * Zwei verschiedene Loesungen, weil die beiden Aufrufstellen verschieden sind:
 *
 * - `sha256Hex` ist asynchron und nimmt `crypto.subtle`, wenn es da ist. Der
 *   IR-Builder wartet ohnehin auf den Quelltext, also kostet das nichts. Der
 *   mitgelieferte Fallback ist keine Zierde: `crypto.subtle` existiert nur in
 *   einem sicheren Kontext, und http://127.0.0.1 ist zwar einer, ein spaeter
 *   ueber das LAN geoeffnetes UI aber nicht mehr. Ohne Fallback waere dort
 *   ploetzlich jeder snippetHash weg, und eine fehlende Invalidierung faellt
 *   niemandem auf, bis der Cache falsch antwortet.
 * - `sha1Hex` ist synchron, weil `generateChecklist` synchron und rein ist.
 *   Es asynchron zu machen hiesse, die Reinheit der einen Funktion aufzugeben,
 *   deren Ids in `understanding.json` landen.
 *
 * Beide Implementierungen sind gegen die Vektoren aus RFC 6234 und gegen
 * `node:crypto` geprueft (siehe hash.test.ts). Die Ids und Hashes sind damit
 * dieselben, die das Referenzprojekt erzeugt, was der ganze Punkt ist: ein
 * Haken, der in der IDE gesetzt wurde, muss hier derselbe Haken sein.
 */

const encoder = new TextEncoder();

function toHex(words: Uint32Array): string {
    let out = '';
    for (const word of words) {
        out += (word >>> 0).toString(16).padStart(8, '0');
    }
    return out;
}

/**
 * Anhaengen der Laenge nach der Merkle-Damgard-Vorschrift, die sha1 und sha256
 * teilen: ein 0x80-Byte, Nullen bis 56 mod 64, dann die Bitlaenge big-endian.
 */
function padded(bytes: Uint8Array): Uint32Array {
    const bitLength = bytes.length * 8;
    const blocks = Math.ceil((bytes.length + 9) / 64);
    const words = new Uint32Array(blocks * 16);
    for (let i = 0; i < bytes.length; i += 1) {
        words[i >> 2] |= bytes[i] << (24 - (i % 4) * 8);
    }
    words[bytes.length >> 2] |= 0x80 << (24 - (bytes.length % 4) * 8);
    // Die obere Haelfte der 64-Bit-Laenge bleibt null: eine Eingabe jenseits
    // von 512 MB kommt hier nicht vor, und sie zu behaupten waere schlimmer,
    // als sie nicht zu unterstuetzen.
    words[blocks * 16 - 1] = bitLength >>> 0;
    words[blocks * 16 - 2] = Math.floor(bitLength / 0x100000000);
    return words;
}

const rotl = (value: number, by: number): number => (value << by) | (value >>> (32 - by));
const rotr = (value: number, by: number): number => (value >>> by) | (value << (32 - by));

/** sha1 einer Bytefolge, synchron. */
function sha1Bytes(bytes: Uint8Array): string {
    const words = padded(bytes);
    const h = new Uint32Array([0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476, 0xc3d2e1f0]);
    const w = new Uint32Array(80);

    for (let block = 0; block < words.length; block += 16) {
        for (let i = 0; i < 16; i += 1) {
            w[i] = words[block + i];
        }
        for (let i = 16; i < 80; i += 1) {
            w[i] = rotl(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }
        let [a, b, c, d, e] = [h[0], h[1], h[2], h[3], h[4]];
        for (let i = 0; i < 80; i += 1) {
            let f: number;
            let k: number;
            if (i < 20) {
                f = (b & c) | (~b & d);
                k = 0x5a827999;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ed9eba1;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8f1bbcdc;
            } else {
                f = b ^ c ^ d;
                k = 0xca62c1d6;
            }
            const temp = (rotl(a, 5) + f + e + k + w[i]) >>> 0;
            e = d;
            d = c;
            c = rotl(b, 30);
            b = a;
            a = temp;
        }
        h[0] = (h[0] + a) >>> 0;
        h[1] = (h[1] + b) >>> 0;
        h[2] = (h[2] + c) >>> 0;
        h[3] = (h[3] + d) >>> 0;
        h[4] = (h[4] + e) >>> 0;
    }
    return toHex(h);
}

const SHA256_K = new Uint32Array([
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

/** sha256 einer Bytefolge, synchron. Der Fallback fuer `crypto.subtle`. */
function sha256Bytes(bytes: Uint8Array): string {
    const words = padded(bytes);
    const h = new Uint32Array([
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ]);
    const w = new Uint32Array(64);

    for (let block = 0; block < words.length; block += 16) {
        for (let i = 0; i < 16; i += 1) {
            w[i] = words[block + i];
        }
        for (let i = 16; i < 64; i += 1) {
            const s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >>> 3);
            const s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >>> 10);
            w[i] = (w[i - 16] + s0 + w[i - 7] + s1) >>> 0;
        }
        let [a, b, c, d, e, f, g, hh] = [h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]];
        for (let i = 0; i < 64; i += 1) {
            const s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            const ch = (e & f) ^ (~e & g);
            const temp1 = (hh + s1 + ch + SHA256_K[i] + w[i]) >>> 0;
            const s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            const maj = (a & b) ^ (a & c) ^ (b & c);
            const temp2 = (s0 + maj) >>> 0;
            hh = g;
            g = f;
            f = e;
            e = (d + temp1) >>> 0;
            d = c;
            c = b;
            b = a;
            a = (temp1 + temp2) >>> 0;
        }
        const next = [a, b, c, d, e, f, g, hh];
        for (let i = 0; i < 8; i += 1) {
            h[i] = (h[i] + next[i]) >>> 0;
        }
    }
    return toHex(h);
}

/** sha1 eines Textes als Hex, synchron. */
export function sha1Hex(text: string): string {
    return sha1Bytes(encoder.encode(text));
}

/** sha256 eines Textes als Hex, ohne WebCrypto. Nur fuer Tests und den Fallback. */
export function sha256HexSync(text: string): string {
    return sha256Bytes(encoder.encode(text));
}

/**
 * sha256 eines Textes als Hex.
 *
 * Nimmt `crypto.subtle`, wo es das gibt, und sonst die eigene Rechnung. Beide
 * Wege liefern dieselben 64 Hexziffern; welcher gelaufen ist, darf an keiner
 * Aufrufstelle sichtbar sein.
 */
export async function sha256Hex(text: string): Promise<string> {
    const subtle = globalThis.crypto?.subtle;
    if (subtle !== undefined) {
        try {
            const digest = await subtle.digest('SHA-256', encoder.encode(text));
            return [...new Uint8Array(digest)]
                .map((byte) => byte.toString(16).padStart(2, '0'))
                .join('');
        } catch {
            // Ein unsicherer Kontext meldet sich hier und nicht bei der
            // Feature-Erkennung. Der Fallback ist die Antwort darauf.
        }
    }
    return sha256HexSync(text);
}
