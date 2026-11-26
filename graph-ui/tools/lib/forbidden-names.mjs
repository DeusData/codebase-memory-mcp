/**
 * Die Zeichen und Namen, die in diesem Repository nicht vorkommen sollen.
 *
 * ## Warum die Namen hier aus Zeichencodes gebaut werden
 *
 * Ein Waechter, der einen Namen sucht, muss den Namen kennen. Steht er als
 * Zeichenkette in der Datei, findet der Waechter beim naechsten Lauf sich
 * selbst und meldet einen Verstoss, den es nicht gibt. Der uebliche Ausweg ist,
 * die eigene Datei von der Pruefung auszunehmen, und der ist schlechter: eine
 * ausgenommene Datei ist genau der Ort, an dem sich ein echter Verstoss
 * verstecken koennte.
 *
 * Also stehen die Namen als Zeichencodes da. Das ist kein Versteck: das
 * Stil-Gate schreibt die aufgeloesten Namen in sein Ergebnis
 * (`namesWatched` in verification/w6/stylegate.json), und `node -p` zeigt sie
 * in einer Zeile. Nachlesbar bleibt alles, nur nicht auffindbar fuer den
 * eigenen Scan.
 *
 * ## Die langen Striche
 *
 * U+2013 und U+2014 stehen aus demselben Grund als Escape-Sequenz und nicht als
 * Zeichen. Hier ist es sogar die uebliche Schreibweise: ein Zeichen, das man in
 * einem Editor nicht von einem Bindestrich unterscheiden kann, gehoert als Code
 * geschrieben, damit jeder Leser sieht, welches der drei gemeint ist.
 */

/** U+2013 (en dash) und U+2014 (em dash). Diese beiden und keine anderen. */
export const LONG_DASH = /[\u2013\u2014]/;

/**
 * Die Namen, die kein Beitraegender dieses Repositories ist.
 *
 * Der Produktname des Assistenz-Werkzeugs und der Firmenname dahinter.
 */
export const WATCHED_NAMES = [
    String.fromCharCode(99, 108, 97, 117, 100, 101),
    String.fromCharCode(97, 110, 116, 104, 114, 111, 112, 105, 99),
];

/** Der blosse Name, ohne Ansehen der Gross-/Kleinschreibung. */
export const NAME_PATTERN = new RegExp(WATCHED_NAMES.join('|'), 'i');

/**
 * Der Verweis auf die Regeldatei dieses Projekts, deren Name den Werkzeugnamen
 * traegt. Eine Zeile, die sie nennt, verweist auf die Regel und behauptet
 * keine Urheberschaft.
 */
export const RULE_FILE_PATTERN = new RegExp(`(${WATCHED_NAMES.join('|')})\\.md\\b`, 'i');

const name = WATCHED_NAMES.join('|');

/**
 * Muster, die eine Urheberschaft behaupten. Davon gibt es keine Ausnahme.
 *
 * Jedes verlangt den Namen UND eine Urheberschafts-Formel in derselben Zeile.
 * Ein Muster, das nur "generated with" verlangte, faende jeden Satz ueber ein
 * erzeugtes Bundle; ein Muster, das nur den Namen verlangte, faende das Verbot
 * selbst. Erst beides zusammen ist eine Attribution.
 */
export const ATTRIBUTION_PATTERNS = [
    {
        name: 'co-authored-by',
        pattern: new RegExp(`co-authored-by\\s*:.*(${name})`, 'i'),
    },
    {
        name: 'generated-with',
        pattern: new RegExp(`generated\\s+with\\s+[^\\n]*(${name})`, 'i'),
    },
    {
        name: 'robot-generated',
        pattern: /\u{1F916}\s*generated/iu,
    },
    {
        name: 'mention',
        pattern: new RegExp(`@(${name})\\b`, 'i'),
    },
    {
        name: 'vendor-mail',
        pattern: new RegExp(`[\\w.+-]*@[\\w.-]*(${name})[\\w.-]*\\.[a-z]{2,}`, 'i'),
    },
    {
        name: 'written-by',
        pattern: new RegExp(`(written|authored|created)\\s+by\\s+(a\\s+|the\\s+)?(${name})`, 'i'),
    },
    {
        name: 'authored-by',
        pattern: new RegExp(`\\b(${name})\\s+(wrote|contributed|authored|generated)\\b`, 'i'),
    },
];
