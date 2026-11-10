import { typoBudget, withinEdits } from './relationship-answer';

/** Whether a model answer only restates its question (H2).
 *
 * With .github selected, "kkannst du mir die heirarchie erklären" got "Ich kann dir die Heirarchy
 * erklären." and "erkläre die aktuelle hierarchy" got "Die aktuelle Hierarchie erklärt.": nothing
 * but the words of the question. Such an answer is no answer. A short real one stays: "Ja, 11."
 * answers a count, "Ja." or "No." a yes or no question, "Das ist ein Ordner." a "was ist". */

/** Words that carry nothing of their own in either language: pronouns, articles, auxiliaries, politeness. */
const STOP = new Set([
    'ich', 'du', 'er', 'sie', 'es', 'wir', 'ihr', 'mir', 'dir', 'mich', 'dich', 'uns', 'euch', 'ihm', 'ihn', 'ihnen', 'der', 'die', 'das', 'den', 'dem', 'des', 'ein', 'eine',
    'einen', 'einem', 'einer', 'eines', 'und', 'oder', 'aber', 'ist', 'sind', 'war', 'bin', 'bist', 'hat', 'habe', 'hast', 'haben', 'kann', 'kannst', 'können', 'könnte',
    'könntest', 'will', 'werde', 'wird', 'werden', 'gerne', 'gern', 'natürlich', 'klar', 'sicher', 'hier', 'da', 'dies', 'diese', 'dieser', 'dieses', 'diesen', 'diesem',
    'mal', 'bitte', 'so', 'auch', 'noch', 'nur', 'zu', 'mit', 'von', 'für', 'auf', 'in', 'im', 'an', 'am', 'um', 'was', 'wie', 'wo', 'wer', 'dass', 'jetzt', 'gerade', 'dazu',
    'i', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'my', 'your', 'us', 'the', 'a', 'an', 'this', 'that', 'these', 'those', 'is', 'are', 'was', 'be', 'am', 'can', 'could',
    'will', 'would', 'shall', 'should', 'do', 'does', 'did', 'of', 'to', 'for', 'in', 'on', 'at', 'by', 'with', 'and', 'or', 'sure', 'certainly', 'course', 'happy', 'glad',
    'here', 'there', 'let', 'what', 'how', 'ok', 'okay', 'please', 'so', 'just', 'now',
]);
/** A reply to a yes or no question. */
const YES_NO = new Set(['ja', 'nein', 'doch', 'yes', 'no', 'nope', 'yep']);
/** "I can explain X", "Ich kann dir X erklären", "Gerne erkläre ich ...": an offer, not an answer. */
const RESTATEMENT = /(?:^|[^\p{L}])(?:i can|i could|i will|i'll|i would|i'd|let me|happy to|glad to|ich kann|kann ich|ich könnte|könnte ich|ich werde|werde ich|ich erkläre|erkläre ich|gerne|gern)(?=[^\p{L}]|$)/iu;
/** Words a restatement may add without saying anything: "die aktuelle Struktur des Projekts". */
const RESTATED_WORDS = 3;

const wordsOf = (text: string): string[] => (text.match(/`[^`]+`|[\p{L}\p{N}_][\p{L}\p{N}_.:/-]*/gu) ?? []).map(word => word.replace(/[.:/-]+$/, '')).filter(Boolean);
/** A name, path or number: something the question did not say is information, however short the answer. */
const codeShaped = (word: string): boolean => /^`|\p{N}|_|[./:]|\p{Ll}\p{Lu}|\p{Lu}{2}\p{Ll}/u.test(word);
/** The same word, mistyped or inflected: "Heirarchy" for "heirarchie", "erklärt" for "erkläre". */
function same(left: string, right: string): boolean {
    if (left === right) return true;
    const longer = left.length >= right.length ? left : right, shorter = longer === left ? right : left;
    if (shorter.length >= 4 && withinEdits(left, right, typoBudget(longer))) return true;
    let prefix = 0;
    while (prefix < shorter.length && left[prefix] === right[prefix]) prefix++;
    return prefix >= 4 && prefix >= shorter.length - 2;
}

export function echoAnswer(answer: string, question: string): boolean {
    const words = wordsOf(answer);
    if (!words.length) return false;
    const asked = wordsOf(question).map(word => word.toLowerCase().replace(/`/g, ''));
    const content = words.filter(word => {
        const lower = word.toLowerCase().replace(/`/g, '');
        return !STOP.has(lower) && !YES_NO.has(lower) && !asked.some(other => same(lower, other));
    });
    if (content.some(codeShaped)) return false;
    if (RESTATEMENT.test(answer)) return content.length < RESTATED_WORDS;
    return content.length === 0 && !words.some(word => YES_NO.has(word.toLowerCase()));
}
