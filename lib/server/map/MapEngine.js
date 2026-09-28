import { rgba, formatTimeHeld, timeHeldColor } from './data.js';
const MAP_OFFSET_X = 2558;
const MAP_OFFSET_Z = 6638;
const MIN_ZOOM = 0.06;
const MAX_ZOOM = 14;
const FILL_ALPHA = 80 / 255;
const BORDER_WIDTH = 2.5;
const LABEL_FONT = '15px Minecraft, monospace';
const TIMER_FONT = '12px Minecraft, monospace';
const CROWN_SIZE = 36;
const COOLDOWN_MS = 10 * 60 * 1000;

export class MapEngine {
	constructor(canvas, callbacks = {}) {
		this.canvas = canvas;
		this.ctx = canvas.getContext('2d');
		this.callbacks = callbacks;
		this.spawnMode = false;
		this.mapImage = null; 
		this.crownImage = null;
		this.territories = [];
		this.routes = []; 
		this.resourceMode = false;
		this.showTimers = false; 
		this.timeRef = null;
		this._timerTickAt = 0;
		this._cooldownVisible = false;
		this.camX = -150;
		this.camZ = -2000;
		this.zoom = 0.25;
		this.targetZoom = 0.25;
		this.zoomAnchor = null;
		this.hovered = null;
		this.dragging = false;
		this.dragMoved = 0;
		this.lastPointer = null;
		this.mouse = { x: 0, y: 0, inside: false };

		this.dirty = true;
		this.running = true;

		this._bindEvents();
		this._resize();
		this._resizeObserver = new ResizeObserver(() => this._resize());
		this._resizeObserver.observe(canvas);

		const loop = () => {
			if (!this.running) return;
			this._tick();
			requestAnimationFrame(loop);
		};
		requestAnimationFrame(loop);
	}

	destroy() {
		this.running = false;
		this._resizeObserver.disconnect();
		this._unbindEvents();
	}
	setMapImage(bitmap) {
		this.mapImage = bitmap;
		this.dirty = true;
	}

	setTimersVisible(visible) {
		this.showTimers = visible;
		this.dirty = true;
	}
	setTimeReference(ms) {
		this.timeRef = ms;
		this.dirty = true;
	}

	setCrownImage(img) {
		this.crownImage = img;
		this.dirty = true;
	}

	setTerritories(territories) {
		this.territories = territories;
		if (this.hovered) {
			this.hovered = territories.find((t) => t.name === this.hovered.name) ?? null;
		}
		const byName = new Map(territories.map((t) => [t.name, t]));
		const seen = new Set();
		const routes = [];
		for (const t of territories) {
			for (const link of t.links) {
				const other = byName.get(link);
				if (!other) continue;
				const key = t.name < link ? t.name + '\0' + link : link + '\0' + t.name;
				if (seen.has(key)) continue;
				seen.add(key);
				routes.push([t, other]);
			}
		}
		this.routes = routes;
		this.dirty = true;
	}

	setView(wx, wz, zoom) {
		this.camX = wx;
		this.camZ = wz;
		this.zoom = this.targetZoom = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, zoom));
		this.dirty = true;
	}

	setResourceMode(enabled) {
		this.resourceMode = enabled;
		this._updateHover();
		this.dirty = true;
	}

	centerOnWorld() {
		this.camX = -300;
		this.camZ = -1800;
		const fit = Math.min(
			this.canvas.clientWidth / 4800,
			this.canvas.clientHeight / 6700
		);
		this.zoom = this.targetZoom = Math.max(fit * 1.6, MIN_ZOOM);
		this.dirty = true;
	}

	screenToWorld(sx, sy) {
		const w = this.canvas.clientWidth;
		const h = this.canvas.clientHeight;
		return [
			(sx - w / 2) / this.zoom + this.camX,
			(sy - h / 2) / this.zoom + this.camZ
		];
	}

	worldToScreen(wx, wz) {
		const w = this.canvas.clientWidth;
		const h = this.canvas.clientHeight;
		return [(wx - this.camX) * this.zoom + w / 2, (wz - this.camZ) * this.zoom + h / 2];
	}

	_bindEvents() {
		const c = this.canvas;
        this._touches = new Map();
		this._onPointerDown = (e) => {
			if (e.button !== 0) return;
            const rect = c.getBoundingClientRect();
            this.mouse = {x:e.clientX-rect.left, y:e.clientY-rect.top, inside:true};
            this._updateHover();
            if (e.pointerType === 'touch') {
                this._touches.set(e.pointerId, [e.clientX, e.clientY]);
                if (this._touches.size > 1) { this.dragMoved = 100; return; }
            }
			c.setPointerCapture(e.pointerId);
			this.dragging = true;
			this.dragMoved = 0;
			this.lastPointer = [e.clientX, e.clientY];
		};
		this._onPointerMove = (e) => {
            if (this._touches.has(e.pointerId)) {
                const before = [...this._touches.values()];
                this._touches.set(e.pointerId, [e.clientX, e.clientY]);
                if (before.length === 2) {
                    const after = [...this._touches.values()];
                    const distance = p => Math.hypot(p[0][0]-p[1][0], p[0][1]-p[1][1]);
                    const rect = c.getBoundingClientRect();
                    const x = (before[0][0]+before[1][0])/2-rect.left, y=(before[0][1]+before[1][1])/2-rect.top;
                    const [wx,wz] = this.screenToWorld(x,y);
                    this.zoom = this.targetZoom = Math.max(MIN_ZOOM,Math.min(MAX_ZOOM,this.zoom*distance(after)/Math.max(1,distance(before))));
                    this.zoomAnchor = null;
                    this.camX = wx-((after[0][0]+after[1][0])/2-rect.left-c.clientWidth/2)/this.zoom;
                    this.camZ = wz-((after[0][1]+after[1][1])/2-rect.top-c.clientHeight/2)/this.zoom;
                    this.dirty=true; this.dragMoved=100; return;
                }
            }
			const rect = c.getBoundingClientRect();
			this.mouse = { x: e.clientX - rect.left, y: e.clientY - rect.top, inside: true };

			if (this.dragging && this.lastPointer) {
				const dx = e.clientX - this.lastPointer[0];
				const dy = e.clientY - this.lastPointer[1];
				this.dragMoved += Math.abs(dx) + Math.abs(dy);
				this.camX -= dx / this.zoom;
				this.camZ -= dy / this.zoom;
				this.lastPointer = [e.clientX, e.clientY];
				this.dirty = true;
			}
			this._updateHover();
			this.callbacks.onCursorMove?.(...this.screenToWorld(this.mouse.x, this.mouse.y));
		};
		this._onPointerUp = (e) => {
            this._touches.delete(e.pointerId);
			if (e.button !== 0) return;
			this.dragging = false;
			if (this.dragMoved < 6) {
				this.callbacks.onClick?.(this.hovered);
			}
		};
		this._onPointerLeave = () => {
			this.mouse.inside = false;
			if (this.hovered !== null) {
				this.hovered = null;
				this.callbacks.onHover?.(null);
				this.dirty = true;
			}
			this.callbacks.onCursorMove?.(null, null);
		};
		this._onWheel = (e) => {
			e.preventDefault();
			const rect = c.getBoundingClientRect();
			const sx = e.clientX - rect.left;
			const sy = e.clientY - rect.top;
			const factor = Math.pow(1.0017, -e.deltaY);
			this.targetZoom = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, this.targetZoom * factor));
			const [wx, wz] = this.screenToWorld(sx, sy);
			this.zoomAnchor = { sx, sy, wx, wz };
			this.dirty = true;
		};
		this._onContextMenu = (e) => e.preventDefault();

		c.addEventListener('pointerdown', this._onPointerDown);
		c.addEventListener('pointermove', this._onPointerMove);
		c.addEventListener('pointerup', this._onPointerUp);
        this._onPointerCancel = () => { this._touches.clear(); this.dragging=false; this.lastPointer=null; };
        c.addEventListener('pointercancel', this._onPointerCancel);
		c.addEventListener('pointerleave', this._onPointerLeave);
		c.addEventListener('wheel', this._onWheel, { passive: false });
		c.addEventListener('contextmenu', this._onContextMenu);
	}

	_unbindEvents() {
		const c = this.canvas;
		c.removeEventListener('pointerdown', this._onPointerDown);
		c.removeEventListener('pointermove', this._onPointerMove);
		c.removeEventListener('pointerup', this._onPointerUp);
        c.removeEventListener('pointercancel', this._onPointerCancel);
		c.removeEventListener('pointerleave', this._onPointerLeave);
		c.removeEventListener('wheel', this._onWheel);
		c.removeEventListener('contextmenu', this._onContextMenu);
	}

	_resize() {
		const dpr = window.devicePixelRatio || 1;
		const w = this.canvas.clientWidth;
		const h = this.canvas.clientHeight;
		if (w === 0 || h === 0) return;
		this.canvas.width = Math.round(w * dpr);
		this.canvas.height = Math.round(h * dpr);
		this.dpr = dpr;
		this.dirty = true;
	}

	_updateHover() {
		if (!this.mouse.inside || this.dragging) return;

		const [wx, wz] = this.screenToWorld(this.mouse.x, this.mouse.y);
		let newHover = null;
		let bestArea = Infinity;
		for (const t of this.territories) {
			if (wx < t.x1 || wx > t.x2 || wz < t.z1 || wz > t.z2) continue;
			if (t.spawn && Math.hypot(wx - t.cx, wz - t.cz) > t.radius) continue;
			const area = (t.x2 - t.x1) * (t.z2 - t.z1);
			if (area < bestArea) {
				newHover = t;
				bestArea = area;
			}
		}
		if (newHover !== this.hovered) {
			this.hovered = newHover;
			this.callbacks.onHover?.(newHover);
			this.dirty = true;
		}
	}

	_tick() {
		if (Math.abs(this.targetZoom - this.zoom) > 1e-4) {
			this.zoom += (this.targetZoom - this.zoom) * 0.35;
			if (Math.abs(this.targetZoom - this.zoom) < 1e-4) this.zoom = this.targetZoom;
			if (this.zoomAnchor) {
				const { sx, sy, wx, wz } = this.zoomAnchor;
				const w = this.canvas.clientWidth;
				const h = this.canvas.clientHeight;
				this.camX = wx - (sx - w / 2) / this.zoom;
				this.camZ = wz - (sy - h / 2) / this.zoom;
			}
			this._updateHover();
			this.dirty = true;
		} else {
			this.zoomAnchor = null;
		}
		if (this._cooldownVisible) {
			this.dirty = true;
		}
		if (this.timeRef === null && performance.now() - this._timerTickAt > 1000) {
			this._timerTickAt = performance.now();
			if (this.showTimers || this.territories.some((t) => this._onCooldown(t))) {
				this.dirty = true;
			}
		}

		if (!this.dirty) return;
		this.dirty = false;
		this._render();
	}

	_render() {
		const ctx = this.ctx;
		const w = this.canvas.clientWidth;
		const h = this.canvas.clientHeight;
		ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);

		ctx.fillStyle = '#000000';
		ctx.fillRect(0, 0, w, h);

		if (this.mapImage) {
			const [sx, sy] = this.worldToScreen(-MAP_OFFSET_X, -MAP_OFFSET_Z);
			ctx.imageSmoothingEnabled = this.zoom < 1;
			ctx.imageSmoothingQuality = 'high';
			ctx.drawImage(
				this.mapImage,
				sx,
				sy,
				this.mapImage.width * this.zoom,
				this.mapImage.height * this.zoom
			);
		}

		if (this.spawnMode) this._renderSpawns(ctx, w, h);
		else this._renderTerritories(ctx, w, h);
	}

    _renderSpawns(ctx, w, h) {
        for (const t of this.territories) {
            const [x, y] = this.worldToScreen(t.cx, t.cz);
            const r = Math.max(3, t.radius * this.zoom);
            if (x + r < 0 || y + r < 0 || x - r > w || y - r > h) continue;
            ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2);
            ctx.fillStyle = rgba(t.guildColor, t === this.hovered ? 0.55 : 0.24);
            ctx.strokeStyle = t.guildColor; ctx.lineWidth = t === this.hovered ? 3 : 1.5;
            ctx.fill(); ctx.stroke();
        }
    }

	_onCooldown(t) {
		const ref = this.timeRef ?? Date.now();
		return !!t.acquired && ref >= t.acquired && ref - t.acquired < COOLDOWN_MS;
	}

	_renderTerritories(ctx, w, h) {
		const onScreen = [];
		for (const t of this.territories) {
			const [sx1, sy1] = this.worldToScreen(t.x1, t.z1);
			const [sx2, sy2] = this.worldToScreen(t.x2, t.z2);
			t._sx1 = sx1;
			t._sy1 = sy1;
			t._sx2 = sx2;
			t._sy2 = sy2;
			if (sx2 < -50 || sy2 < -50 || sx1 > w + 50 || sy1 > h + 50) continue;
			onScreen.push(t);
		}
		const onScreenSet = new Set(onScreen);
		ctx.strokeStyle = '#101010';
		ctx.lineWidth = 1.5;
		ctx.beginPath();
		for (const [a, b] of this.routes) {
			if (!onScreenSet.has(a) && !onScreenSet.has(b)) continue;
			const [ax, ay] = this.worldToScreen(a.cx, a.cz);
			const [bx, by] = this.worldToScreen(b.cx, b.cz);
			ctx.moveTo(ax, ay);
			ctx.lineTo(bx, by);
		}
		ctx.stroke();

		let cooldownVisible = false;
		for (const t of onScreen) {
			const colors = this.resourceMode ? t.resourceColors : [t.guildColor];
			const x = t._sx1;
			const y = t._sy1;
			const rw = t._sx2 - t._sx1;
			const rh = t._sy2 - t._sy1;

			if (colors.length === 1) {
				ctx.fillStyle = rgba(colors[0], FILL_ALPHA);
			} else {
				const grad = ctx.createLinearGradient(x, y, x + rw, y);
				colors.forEach((c, i) => grad.addColorStop(i / (colors.length - 1), rgba(c, FILL_ALPHA)));
				ctx.fillStyle = grad;
			}
			ctx.fillRect(x, y, rw, rh);

			if (colors.length === 1) {
				ctx.strokeStyle = colors[0];
			} else {
				const grad = ctx.createLinearGradient(x, y, x + rw, y);
				colors.forEach((c, i) => grad.addColorStop(i / (colors.length - 1), c));
				ctx.strokeStyle = grad;
			}
			ctx.lineWidth = BORDER_WIDTH;
			if (this._onCooldown(t)) {
				cooldownVisible = true;
				ctx.setLineDash([10, 6]);
				ctx.lineDashOffset = -((performance.now() / 30) % 16);
				ctx.globalAlpha = 0.55;
				ctx.strokeRect(x, y, rw, rh);
				ctx.globalAlpha = 1;
				ctx.setLineDash([]);
				ctx.lineDashOffset = 0;
			} else {
				ctx.strokeRect(x, y, rw, rh);
			}
		}
		this._cooldownVisible = cooldownVisible;

		ctx.textAlign = 'center';
		ctx.textBaseline = 'middle';
		ctx.lineJoin = 'round';
		for (const t of onScreen) {
			const cx = (t._sx1 + t._sx2) / 2;
			const cy = (t._sy1 + t._sy2) / 2;

			if (t.hq && this.crownImage) {
				const cw = Math.min(CROWN_SIZE, Math.max(14, t._sx2 - t._sx1));
				const ch = cw * (this.crownImage.height / this.crownImage.width);
				ctx.imageSmoothingEnabled = false;
				ctx.drawImage(this.crownImage, cx - cw / 2, cy - ch / 2, cw, ch);
			} else {
				const colors = this.resourceMode ? t.resourceColors : [t.guildColor];
				ctx.font = LABEL_FONT;
				ctx.strokeStyle = '#000000';
				ctx.lineWidth = 4;
				ctx.strokeText(t.prefix, cx, cy);
				ctx.fillStyle = colors[0];
				ctx.fillText(t.prefix, cx, cy);
			}

			if (t === this.hovered) {
				ctx.font = LABEL_FONT;
				ctx.strokeStyle = '#000000';
				ctx.lineWidth = 4;
				ctx.strokeText(t.name, cx, t._sy1 + 10);
				ctx.fillStyle = '#ffffff';
				ctx.fillText(t.name, cx, t._sy1 + 10);
			}
		}
		if (this.showTimers) {
			const ref = this.timeRef ?? Date.now();
			ctx.font = TIMER_FONT;
			ctx.textAlign = 'center';
			ctx.textBaseline = 'bottom';
			for (const t of onScreen) {
				if (!t.acquired) continue;
				if (t._sx2 - t._sx1 < 56 || t._sy2 - t._sy1 < 40) continue; 
				const text = formatTimeHeld(t.acquired, ref, true);
				const cx = (t._sx1 + t._sx2) / 2;
				ctx.strokeStyle = '#000000';
				ctx.lineWidth = 4;
				ctx.strokeText(text, cx, t._sy2 - 4);
				ctx.fillStyle = timeHeldColor(t.acquired, ref);
				ctx.fillText(text, cx, t._sy2 - 4);
			}
			ctx.textBaseline = 'middle';
		}
	}

}
