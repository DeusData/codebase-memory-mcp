/** Build-time opt-in; URL parameters and saved preferences cannot enable it. */
export const experimentalAgentsEnabled = import.meta.env.VITE_EXPERIMENTAL_AGENTS === 'true';
