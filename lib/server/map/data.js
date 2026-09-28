const MC = {
	DARK_GREEN: '#00AA00',
	DARK_RED: '#AA0000',
	GOLD: '#FFAA00',
	GREEN: '#55FF55',
	AQUA: '#55FFFF',
	RED: '#FF5555',
	YELLOW: '#FFFF55',
	WHITE: '#FFFFFF'
};

export const RESOURCES = {
	EMERALD: { name: 'Emeralds', icon: null, color: MC.GREEN },
	ORE: { name: 'Ore', icon: '/icons/resources/ore.png', color: MC.WHITE },
	WOOD: { name: 'Wood', icon: '/icons/resources/wood.png', color: MC.GOLD },
	FISH: { name: 'Fish', icon: '/icons/resources/fish.png', color: MC.AQUA },
	CROP: { name: 'Crops', icon: '/icons/resources/crop.png', color: MC.YELLOW }
};

export const RESOURCE_ORDER = ['EMERALD', 'ORE', 'WOOD', 'FISH', 'CROP'];

function hsv(h, s, v) {
	const i = Math.floor(h * 6);
	const f = h * 6 - i;
	const p = v * (1 - s);
	const q = v * (1 - f * s);
	const t = v * (1 - (1 - f) * s);
	let r, g, b;
	switch (i % 6) {
		case 0: [r, g, b] = [v, t, p]; break;
		case 1: [r, g, b] = [q, v, p]; break;
		case 2: [r, g, b] = [p, v, t]; break;
		case 3: [r, g, b] = [p, q, v]; break;
		case 4: [r, g, b] = [t, p, v]; break;
		default: [r, g, b] = [v, p, q]; break;
	}
	return (
		'#' +
		[r, g, b].map((c) => Math.round(c * 255).toString(16).padStart(2, '0')).join('')
	);
}

const RESOURCE_TERRITORY_COLORS = {
	ORE: hsv(0, 0.3, 1),
	FISH: hsv(0.5, 0.6, 0.9),
	WOOD: hsv(1 / 3, 0.6, 0.9),
	CROP: hsv(1 / 6, 0.6, 0.9)
};

export const LEVELS = ['VERY_LOW', 'LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH'];

export const LEVEL_INFO = {
	NONE: { label: 'None', defence: MC.WHITE, treasury: MC.WHITE },
	VERY_LOW: { label: 'Very Low', defence: MC.DARK_GREEN, treasury: MC.DARK_GREEN },
	LOW: { label: 'Low', defence: MC.GREEN, treasury: MC.GREEN },
	MEDIUM: { label: 'Medium', defence: MC.YELLOW, treasury: MC.YELLOW },
	HIGH: { label: 'High', defence: MC.RED, treasury: MC.RED },
	VERY_HIGH: { label: 'Very High', defence: MC.DARK_RED, treasury: MC.AQUA }
};

const CRC_TABLE = (() => {
	const table = new Uint32Array(256);
	for (let n = 0; n < 256; n++) {
		let c = n;
		for (let k = 0; k < 8; k++) {
			c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
		}
		table[n] = c;
	}
	return table;
})();

function crc32(str) {
	const bytes = new TextEncoder().encode(str);
	let crc = 0xffffffff;
	for (const b of bytes) {
		crc = CRC_TABLE[(crc ^ b) & 0xff] ^ (crc >>> 8);
	}
	return (crc ^ 0xffffffff) >>> 0;
}

const MULT = 0x5deece66dn;
const ADDEND = 0xbn;
const MASK = (1n << 48n) - 1n;

function javaRandomFloat(state) {
	state.s = (state.s * MULT + ADDEND) & MASK;
	return Number(state.s >> 24n) / (1 << 24);
}

const hashColorCache = new Map();

export function colorForStringHash(input) {
	let color = hashColorCache.get(input);
	if (color) return color;

	const state = { s: (BigInt(crc32(input)) ^ MULT) & MASK };
	const minS = 0.5;
	const minV = 0.75;
	const h = javaRandomFloat(state);
	const s = minS + (1 - minS) * javaRandomFloat(state);
	const v = minV + (1 - minV) * javaRandomFloat(state);
	color = hsv(h, s, v);
	hashColorCache.set(input, color);
	return color;
}

function hexToRgb(hex) {
	return [
		parseInt(hex.slice(1, 3), 16),
		parseInt(hex.slice(3, 5), 16),
		parseInt(hex.slice(5, 7), 16)
	];
}

export function rgba(hex, alpha) {
	const [r, g, b] = hexToRgb(hex);
	return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

const MATERIALS = ['ORE', 'FISH', 'WOOD', 'CROP'];
export function processTerritories(raw, guildColors) {
	const territories = [];

	for (const [name, info] of Object.entries(raw)) {
		const loc = info.location;
		if (!loc?.start || !loc?.end) continue;

		const x1 = Math.min(loc.start[0], loc.end[0]);
		const x2 = Math.max(loc.start[0], loc.end[0]);
		const z1 = Math.min(loc.start[1], loc.end[1]);
		const z2 = Math.max(loc.start[1], loc.end[1]);

		const guildName = info.guild?.name ?? null;
		const prefix = info.guild?.prefix ?? '';

		let guildColor = '#ffffff';
		if (guildName) {
			guildColor = guildColors?.[guildName]?.color ?? colorForStringHash(guildName);
		}

		const resources = {};
		for (const r of info.resources ?? []) {
			resources[r.type] = {
				generation: r.generation,
				baseGeneration: r.baseGeneration,
				stored: r.stored,
				limit: r.limit
			};
		}

		const resourceColors = MATERIALS.filter((m) => resources[m]?.generation > 0).map(
			(m) => RESOURCE_TERRITORY_COLORS[m]
		);
		if (resourceColors.length === 0) resourceColors.push('#ffffff');

		territories.push({
			name,
			historyKnown: info.history_known,
			guildName,
			prefix,
			guildColor,
			acquired: info.acquired ? Date.parse(info.acquired) : null,
			hq: info.hq ?? info.isHQ ?? null,
			x1,
			z1,
			x2,
			z2,
			cx: (x1 + x2) / 2,
			cz: (z1 + z2) / 2,
			links: info.links ?? [],
			resources,
			resourceColors,
			treasury: info.treasury?.toUpperCase() ?? (info.history_known === undefined ? 'NONE' : 'UNKNOWN'),
			defences: info.defences?.toUpperCase() ?? (info.history_known === undefined ? 'NONE' : 'UNKNOWN')
		});
	}

	return territories;
}

export function formatTimeHeld(acquiredMs, nowMs = Date.now(), compact = false) {
	if (!acquiredMs) return '-';
	let s = Math.max(0, Math.floor((nowMs - acquiredMs) / 1000));
	const d = Math.floor(s / 86400);
	s -= d * 86400;
	const h = Math.floor(s / 3600);
	s -= h * 3600;
	const m = Math.floor(s / 60);
	s -= m * 60;

	if (compact) {
		if (d) return `${d}d ${h}h`;
		if (h) return `${h}h ${m}m`;
		if (m) return `${m}m ${s}s`;
		return `${s}s`;
	}

	const parts = [];
	if (d) parts.push(`${d}d`);
	if (d || h) parts.push(`${h}h`);
	if (d || h || m) parts.push(`${m}m`);
	parts.push(`${s}s`);
	return parts.join(' ');
}

export function timeHeldColor(acquiredMs, nowMs = Date.now()) {
	if (!acquiredMs) return '#FFFFFF';
	const diff = nowMs - acquiredMs;
	if (diff < 60 * 60 * 1000) return '#55FF55';
	if (diff < 24 * 60 * 60 * 1000) return '#FFFF55';
	return '#FF5555';
}
