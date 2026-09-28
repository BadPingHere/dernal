import { LEVELS, LEVEL_INFO } from './data.js';

const DMG = [0, 0.4, 0.8, 1.2, 1.6, 2.0, 2.4, 2.8, 3.2, 3.6, 4.0, 4.4];
const ATK = [0, 0.5, 1.0, 1.5, 2.2, 3.0, 4.0, 5.0, 6.2, 6.6, 7.4, 8.4];
const HP = [0, 0.5, 1.0, 1.5, 2.2, 3.0, 4.0, 5.2, 6.4, 7.6, 8.8, 10.0];
const DEF = [0, 3, 4.50, 5.25, 6.00, 6.50, 6.90, 7.20, 7.40, 7.60, 7.80, 8.00];

export function calculateTower(stats) {
  const linkMult = 1 + 0.3 * stats.links;
  const hqMult = stats.hq ? 1.5 + 0.25 * stats.ext : 1;

  const dmgMin = 1000 * (1 + DMG[stats.damage]) * linkMult * hqMult;
  const dmgMax = 1500 * (1 + DMG[stats.damage]) * linkMult * hqMult;
  const avgDmg = (dmgMin + dmgMax) / 2;
  const speed = 0.5 * (1 + ATK[stats.attack]);
  const health = 300000 * (1 + HP[stats.health]) * linkMult * hqMult;
  const defense = 0.1 * (1 + DEF[stats.defense]);
  const rating =
    stats.damage +
    stats.attack +
    stats.health +
    stats.defense +
    stats.aura +
    stats.volley +
    (stats.aura > 0 ? 5 : 0) +
    (stats.volley > 0 ? 3 : 0);
  let tier = 1 + (rating > 5) + (rating > 18) + (rating > 30) + (rating > 48);
  if (stats.hq) tier = Math.min(5, tier + 1);

  return {
    dmgMin,
    dmgMax,
    speed,
    health,
    defense,
    dps: avgDmg * speed,
    ehp: health / (1 - defense),
    rating,
    tierName: LEVELS[tier - 1]
  };
}

export function computeTowerBonuses(t, territories) {
  const byName = new Map(territories.map((x) => [x.name, x]));
  let connections = 0;
  let externals = 0;
  const visited = new Set([t.name]);
  let frontier = [t];
  for (let dist = 1; dist <= 3; dist++) {
    const next = [];
    for (const cur of frontier) {
      for (const link of cur.links) {
        if (visited.has(link)) continue;
        visited.add(link);
        const other = byName.get(link);
        if (!other) continue;
        next.push(other);
        if (other.guildName && other.guildName === t.guildName) {
          externals++;
          if (dist === 1) connections++;
        }
      }
    }
    frontier = next;
  }
  return { connections, externals };
}

const rows = [
  ['damage', 'Damage', 11, '#ff5555'], ['attack', 'Attack Speed', 11, '#ffff55'],
  ['health', 'Health', 11, '#55ff55'], ['defense', 'Defense', 11, '#55ffff'],
  ['aura', 'Aura', 3, '#ff55ff'], ['volley', 'Volley', 3, '#ffaa00'],
];
const node = (tag, text, cls) => {
  const element = document.createElement(tag);
  if (text != null) element.textContent = text;
  if (cls) element.className = cls;
  return element;
};

export function createTowerCalculator() {
  const stats = { damage:0, attack:0, health:0, defense:0, aura:0, volley:0, links:0, ext:0, hq:false };
  let selected = null;
  const root = node('section', null, 'tower-calculator');
  root.setAttribute('aria-label', 'Tower Calculator');
  const center = node('div', null, 'tooltip-center');
  const controls = [];
  for (const [key, label, max, color] of rows) {
    const row = node('div', null, 'tower-stat');
    const title = node('span', label); title.style.color = color;
    const stepper = node('span', null, 'tower-stepper');
    const minus = node('button', '−'), value = node('span', '0', 'tower-level'), plus = node('button', '+');
    minus.setAttribute('aria-label', `Decrease ${label}`);
    plus.setAttribute('aria-label', `Increase ${label}`);
    minus.onclick = () => { stats[key] = Math.max(0, stats[key] - 1); render(); };
    plus.onclick = () => { stats[key] = Math.min(max, stats[key] + 1); render(); };
    controls.push(() => { value.textContent = stats[key]; minus.disabled = stats[key] === 0; plus.disabled = stats[key] === max; });
    stepper.append(minus, value, plus); row.append(title, stepper); center.append(row);
  }
  const hqRow = node('div', null, 'tower-stat');
  const hq = node('button', 'No', 'tower-hq');
  hq.setAttribute('aria-label', 'Calculate as headquarters');
  hq.onclick = () => { stats.hq = !stats.hq; render(); };
  hqRow.append(node('span', 'HQ'), hq); center.append(hqRow);
  const bonuses = node('div', null, 'muted');
  const results = node('div', null, 'tower-results');
  results.setAttribute('aria-live', 'polite');
  center.append(node('div', null, 'spacer'), bonuses, results);
  root.append(node('div', null, 'tooltip-top'), center, node('div', 'Tower Calculator', 'tooltip-name'));
  function render() {
    for (const update of controls) update();
    hq.textContent = stats.hq ? 'Yes' : 'No';
    hq.setAttribute('aria-pressed', String(stats.hq));
    const c = calculateTower(stats), fmt = n => Math.round(n).toLocaleString('en-US');
    bonuses.textContent = `${stats.links} Conns, ${stats.ext} Externals`;
    results.replaceChildren();
    for (const [label, value, color] of [
      ['Damage', `${fmt(c.dmgMin)}–${fmt(c.dmgMax)} @ ${c.speed.toFixed(2)}/s`, '#ff5555'],
      ['Health', fmt(c.health), '#55ff55'], ['Defense', `${(c.defense * 100).toFixed(1)}%`, '#55ffff'],
      ['Average DPS', fmt(c.dps), '#ff5555'], ['Effective HP', fmt(c.ehp), '#55ff55'],
      ['Defense Rating', `${LEVEL_INFO[c.tierName].label} (${c.rating})`, LEVEL_INFO[c.tierName].defence],
    ]) {
      const row = node('div', `${label}: `), output = node('span', value);
      output.style.color = color; row.append(output); results.append(row);
    }
  }
  return {
    element: root,
    select(territory, territories) {
      if (!territory) { selected = null; return; }
      if (selected !== territory.name) {
        selected = territory.name;
        const { connections, externals } = computeTowerBonuses(territory, territories);
        stats.links = connections; stats.ext = externals; stats.hq = !!territory.hq;
        render();
      }
    },
  };
}
