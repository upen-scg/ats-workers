export const sleep = (ms) => new Promise(r => setTimeout(r, ms));
export function loopMs(name, fallback) {
  const v = Number(process.env[name]);
  return Number.isFinite(v) && v > 0 ? v : fallback;
}
