/* Visual density compensation.
 *
 * The white-blob-at-scale failure is dominated by EDGES: they blend
 * additively and a 15k-node graph carries ~80k long lines crossing the
 * center, so their glow stacks into an opaque wash. Nodes are far fewer and
 * discrete — their bloom halos are the whole "wow", so we keep those bright.
 *
 * Strategy: dim edges hard (they cause the blob), keep nodes and bloom near
 * full so the bright stars still pop; only ease nodes/bloom back on genuinely
 * huge clouds where even discrete points overlap. Highlighted elements are
 * never scaled — a selection stays bright against the dimmed rest. */

/* Edge counts at/below this render exactly as before. */
export const EDGE_REFERENCE_COUNT = 2500;
const EDGE_MIN_SCALE = 0.05;

export function edgeIntensityScale(edgeCount: number): number {
  if (edgeCount <= EDGE_REFERENCE_COUNT) return 1;
  /* ~1/sqrt(n): 4× the edges → each ~half as bright → total glow ~flat. */
  return Math.max(EDGE_MIN_SCALE, Math.sqrt(EDGE_REFERENCE_COUNT / edgeCount));
}

/* Node glow and bloom stay at full strength up to here — this covers the
 * common "load the whole repo" case (tens of thousands of nodes) so the
 * bright-star look is preserved. */
export const NODE_REFERENCE_COUNT = 25000;
/* …then ease gently toward these floors as the cloud grows past the fade end,
 * where even discrete point sprites start to overlap and over-bloom. */
const NODE_FADE_END = 250000;
const BLOOM_FLOOR = 0.7;
const NODE_BOOST_FLOOR = 0.8;

function fadeFactor(nodeCount: number): number {
  if (nodeCount <= NODE_REFERENCE_COUNT) return 0;
  return Math.min(
    1,
    (nodeCount - NODE_REFERENCE_COUNT) / (NODE_FADE_END - NODE_REFERENCE_COUNT),
  );
}

export function bloomIntensityScale(nodeCount: number): number {
  return 1 - fadeFactor(nodeCount) * (1 - BLOOM_FLOOR);
}

/* Per-node glow boost: full up to the reference count, then a gentle,
 * high-floored fade so large clouds don't merge into mush while moderate
 * graphs keep their halos. */
export function nodeBoostScale(nodeCount: number): number {
  return 1 - fadeFactor(nodeCount) * (1 - NODE_BOOST_FLOOR);
}

/* Colour-aware glow multiplier applied to each node before bloom.
 *
 * Nodes are coloured as star classes by degree: blue giants (high-degree
 * hubs) → white/yellow (mid) → red dwarfs (leaves). Bloom is luminance-
 * thresholded, and blue has a tiny luminance weight, so a naive
 * brightness-based boost makes white/yellow blow out while blue and red stay
 * flat. Instead we boost by *channel dominance*: a blue-dominant node gets the
 * strongest boost (the important hubs shine brightest), a red-dominant node a
 * modest one, and white/yellow — which need no help to bloom — the least.
 *
 * r, g, b are 0..1 colour channels. Returns a multiplier ≥ 1. */
const GLOW_BASE = 1.35;
const GLOW_BLUE_GAIN = 2.4;
const GLOW_RED_GAIN = 0.9;

export function nodeGlowBoost(r: number, g: number, b: number): number {
  /* Blue that exceeds both red and green → cool hub. Red that exceeds both
   * green and blue → warm leaf (green cutoff keeps yellow/orange out of the
   * red term so they are not boosted). */
  const blueness = Math.max(0, b - Math.max(r, g));
  const redness = Math.max(0, r - Math.max(g, b));
  return GLOW_BASE + blueness * GLOW_BLUE_GAIN + redness * GLOW_RED_GAIN;
}

/* ── Manual display settings ──────────────────────────────────── *
 * User-facing multipliers layered ON TOP of the adaptive scales above:
 * the adaptive scale picks a sane default for the current density, the
 * sliders let the user push contrast/brightness around it. Persisted
 * globally (a display preference, not a per-project fact). */

export interface DisplaySettings {
  /** Edge brightness multiplier (0.1–3, default 1). */
  edgeBrightness: number;
  /** Node glow-boost multiplier (0–2, default 1). */
  nodeGlow: number;
  /** Bloom intensity multiplier (0–2, default 1). */
  bloom: number;
}

export const DEFAULT_DISPLAY_SETTINGS: DisplaySettings = {
  edgeBrightness: 1,
  nodeGlow: 1,
  bloom: 1,
};

export const DISPLAY_LIMITS = {
  edgeBrightness: { min: 0.1, max: 3 },
  nodeGlow: { min: 0, max: 2 },
  bloom: { min: 0, max: 2 },
} as const;

const DISPLAY_STORAGE_KEY = "cbm-display";

function clampSetting(key: keyof DisplaySettings, value: unknown): number {
  const { min, max } = DISPLAY_LIMITS[key];
  const n = typeof value === "number" ? value : Number.NaN;
  if (!Number.isFinite(n)) return DEFAULT_DISPLAY_SETTINGS[key];
  return Math.min(max, Math.max(min, n));
}

export function clampDisplaySettings(
  raw: Partial<DisplaySettings>,
): DisplaySettings {
  return {
    edgeBrightness: clampSetting("edgeBrightness", raw.edgeBrightness),
    nodeGlow: clampSetting("nodeGlow", raw.nodeGlow),
    bloom: clampSetting("bloom", raw.bloom),
  };
}

export function loadDisplaySettings(): DisplaySettings {
  try {
    const raw = localStorage.getItem(DISPLAY_STORAGE_KEY);
    if (raw) return clampDisplaySettings(JSON.parse(raw));
  } catch { /* ignore */ }
  return DEFAULT_DISPLAY_SETTINGS;
}

export function saveDisplaySettings(settings: DisplaySettings) {
  try {
    localStorage.setItem(DISPLAY_STORAGE_KEY, JSON.stringify(settings));
  } catch { /* ignore */ }
}
