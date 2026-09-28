import { MapEngine } from "./MapEngine.js";
import { createTowerCalculator } from "./tower.js";
import { processTerritories, formatTimeHeld, LEVELS, LEVEL_INFO, RESOURCES, RESOURCE_ORDER, colorForStringHash } from "./data.js";
import { createTimeline } from './timeline.js';
let cleanup = () => {
};
function closeMap() {
  cleanup();
  cleanup = () => {
  };
}
const node = (tag, text, cls) => {
  const e = document.createElement(tag);
  if (text != null) e.textContent = text;
  if (cls) e.className = cls;
  return e;
};
async function json(url, signal) {
  const r = await fetch(url, { signal });
  if (!r.ok) throw Error("Map update unavailable");
  return r.json();
}
const loadImage = (src) => new Promise((resolve, reject) => {
  const i = new Image();
  i.onload = () => resolve(i);
  i.onerror = () => reject(Error("Map image unavailable"));
  i.src = src;
});
async function showMap(data) {
  closeMap();
  if (!document.querySelector("link[data-map-style]")) {
    const css = node("link");
    css.rel = "stylesheet";
    css.href = "/map/viewer.css";
    css.dataset.mapStyle = "true";
    const loaded = new Promise((resolve, reject) => {
      css.onload = resolve;
      css.onerror = reject;
    });
    document.head.append(css);
    await loaded;
  }
  const root = node("section");
  root.id = "map-viewer";
  root.setAttribute("aria-label", "Interactive Wynncraft map");
  root.innerHTML = `<canvas tabindex="0" aria-label="Map. Drag to pan, scroll to zoom. Search results also navigate the map."></canvas>
 <h1 class="sr-only"></h1><div id="map-status" role="status" class="map-status"></div>
 <button class="toggle-panel mc-panel" aria-expanded="false">Search & filters</button>
 <button id="map-guild-toggle" class="guild-toggle mc-panel" aria-expanded="false">Guilds</button><div id="map-guild-list" class="guild-list mc-panel" hidden></div>
 <aside class="mc-panel" hidden><div class="drawer-head"><span id="map-drawer-title">Search & filters</span><button id="map-close" aria-label="Close search and filters">×</button></div>
 <label for="map-search">Search territories or guilds</label><input id="map-search" placeholder="Search territories, guilds..." autocomplete="off">
 <div id="map-current-controls"><label for="map-color">Territory colors</label><select id="map-color"><option value="guild">Guild ownership</option><option value="resources">Resource generation</option><option value="defense">Defense level</option><option value="treasury">Treasury level</option></select>
 <div class="row"><div><label for="map-defense">Defense filter</label><select id="map-defense"></select></div><div><label for="map-treasury">Treasury filter</label><select id="map-treasury"></select></div></div>
 <label for="map-guild">Guild</label><select id="map-guild"><option value="">All guilds</option></select></div>
 <div id="map-ing-controls" hidden><div class="row"><div><label for="map-tier">Ingredient tier</label><select id="map-tier"><option value="">All tiers</option><option>0</option><option>1</option><option>2</option><option>3</option></select></div><div><label for="map-price">Price floor (EB)</label><input id="map-price" type="number" min="0" placeholder="Any price"></div></div><p class="muted">Cached average lowest prices. Unknown prices are excluded by a price floor.</p></div>
 <div id="map-heat-controls" hidden><label for="map-scale">Heat scale</label><select id="map-scale"><option value="log">Logarithmic</option><option value="linear">Linear</option></select><label for="map-min">Minimum captures</label><input id="map-min" type="number" value="0" min="0"></div>
 <div id="map-summary" class="muted"></div><div id="map-results"></div></aside>
 <div id="map-details" class="tooltip-anchor" hidden></div>
 <div id="map-heat-key" class="heat-key" hidden><div class="tooltip-top"></div><div class="tooltip-center"><div class="heat-title"></div><div class="legend" aria-label="Capture intensity"></div><div class="heat-range"><span>0</span><span id="map-heat-max"></span></div><div id="map-legend" class="muted"></div></div><div class="tooltip-name">Territory Captures</div></div>
 <div class="coords"></div><div class="hotbar-wrap"><div id="map-bar-tooltip" class="mc-panel bar-tooltip" hidden></div><nav class="hotbar" aria-label="Map controls"><span class="cap left"></span>
 <button id="map-resources" class="cell" aria-label="Use Resource Generation colors" aria-pressed="false"></button>
 <button id="map-defense-cycle" class="cell" aria-label="Cycle Territory Defense Filter" aria-pressed="false"></button>
 <button id="map-treasury-cycle" class="cell" aria-label="Cycle Territory Treasury Filter" aria-pressed="false"></button>
 <button id="map-search-open" class="cell" aria-label="Search map"></button>
 <button id="map-settings" class="cell" aria-label="Map filters"></button>
 <button id="map-numbers" class="cell" aria-label="Show territory numbers" aria-pressed="true"></button>
 <button id="map-scale-toggle" class="cell" aria-label="Heat scale: logarithmic. Switch to linear" aria-pressed="true"></button>
 <button id="map-home" class="cell" aria-label="Fit map"></button>
 <button id="map-timers" class="cell" aria-label="Hold timers" aria-pressed="false"></button>
 <button id="map-help" class="cell" aria-label="Quick Guide" aria-pressed="false"></button><span class="cap right"></span></nav></div>
 <div class="zoom-buttons"><button id="map-minus" aria-label="Zoom out">−</button><button id="map-plus" aria-label="Zoom in">+</button></div>
 <div id="map-guide" class="guide mc-panel" hidden><div class="gold">Quick Guide</div><div>Drag to pan · Scroll or pinch to zoom</div><div>Hover for details · Click to pin</div><div>Esc: close panels / unpin details</div><div>/: search · G: guilds · H: hold timers</div><div>Defense / treasury: click to cycle up</div><div>Right click: cycle down · Middle click: reset</div><div>Use the filter icon for colors, tiers and prices</div></div>
 <div class="map-border" aria-hidden="true"></div>`;
  const graphMain = document.querySelector("main");
  if (graphMain) graphMain.hidden = true;
  document.body.append(root);
  const q = (id) => root.querySelector("#map-" + id), abort = new AbortController();
  let timer, disposed = false, raw = data.territories, colors = {}, all = [], shown = [], locked = null, hovered = null;
  let showNumbers = true;
  let colorsLoaded = false;
  let territories = [];
  const timeline = data.map_mode === 'timeline';
  const current = data.map_mode === "current" || timeline, ingredient = data.map_mode === "ingredient", heat = data.map_mode === "heat";
  root.classList.toggle('timeline-map', timeline);
  root.querySelector("h1").textContent = ingredient ? "Ingredient spawns" : heat ? "Territory heatmap" : "Wynncraft territories";
  document.title = root.querySelector("h1").textContent + " / Dernal";
  q("current-controls").hidden = !current;
  q("ing-controls").hidden = !ingredient;
  q("heat-controls").hidden = !heat;
  q("timers").hidden = !current;
  q('search-open').hidden = !ingredient && !timeline;
  q('settings').hidden = !ingredient && !timeline;
  root.querySelector('.toggle-panel').hidden = !ingredient && !timeline;
  q('scale-toggle').hidden = !heat;
  q('numbers').hidden = !heat;
  root.querySelector('.coords').hidden = heat;
  if (!ingredient) {
    const guide=q('guide');
    guide.replaceChildren(node('div','Quick Guide','gold'));
    for(const text of ['Drag to pan · Scroll or pinch to zoom','Hover for details · Click to pin','Esc: close panels / unpin details',
      ...(current ? ['G: guilds · H: hold timers','Defense / treasury: click to cycle up','Right click: cycle down · Middle click: reset'] : ['Use the graph icon to switch logarithmic / linear colors'])]) guide.append(node('div',text));
  }
  for (const id of ['resources','defense-cycle','treasury-cycle','guild-toggle']) q(id).hidden = !current;
  if (timeline) {
    root.querySelector('h1').textContent = 'Historical territory timeline';
    document.title = 'Territory timeline / Dernal';
    q('resources').hidden = true;
    q('treasury-cycle').hidden = true;
    q('treasury').parentElement.hidden = true;
    for (const option of [...q('color').options]) if (['resources','treasury'].includes(option.value)) option.remove();
  }
  q('heat-key').hidden = !heat;
  root.querySelector('.heat-title').textContent = data.timeframe ?? '';
  q('drawer-title').textContent = ingredient ? 'Ingredient spawns' : heat ? 'Capture hotspots' : 'Search & filters';
  if (ingredient) {
    root.querySelector('label[for="map-search"]').textContent = "Search ingredients or mobs";
    q("search").placeholder = "Ingredient or mob name...";
  }
  for (const id of ["defense", "treasury"]) {
    q(id).append(new Option("All levels", ""));
    for (const l of ["NONE", ...LEVELS]) q(id).append(new Option(LEVEL_INFO[l].label, l));
  }
  q("search").value = data.filters?.ingredient ?? "";
  q("tier").value = data.filters?.tier ?? "";
  q("price").value = data.filters?.price ?? "";
  const svg=body=>`<svg viewBox="0 0 16 16" width="22" height="22" aria-hidden="true">${body}</svg>`;
  const toolbarIcon = name => `<img src="/map/assets/icons/toolbar/${name}.png" width="24" height="24" alt="" draggable="false">`;
  const icons={
    resources:toolbarIcon('add_icon'),
    'defense-cycle':toolbarIcon('defense_filter_icon'),
    'treasury-cycle':toolbarIcon('treasury'),
    'search-open':toolbarIcon('search_icon'),
    settings:toolbarIcon('tool'),
    numbers:svg('<text x="8" y="12.5" text-anchor="middle" font-size="13" font-weight="bold" fill="#e8d36b" font-family="monospace">#</text>'),
    'scale-toggle':toolbarIcon('stats_icon'),
    home:toolbarIcon('waypoint_focus_icon'),
    timers:svg('<circle cx="8" cy="8" r="6" fill="none" stroke="#cfd2d8" stroke-width="1.5"/><path d="M8 4 V8 L11 10" stroke="#e8d36b" fill="none" stroke-width="1.5"/>'),
    help:toolbarIcon('help_icon')
  };
  for(const [id,icon] of Object.entries(icons)){
    const button=q(id);button.innerHTML=`<span class="icon">${icon}</span><span class="badge"></span>`;
    const show=()=>{const tip=q('bar-tooltip');tip.replaceChildren(node('div',button.getAttribute('aria-label'),'gold'));if(id.endsWith('-cycle')){tip.append(node('div','Left: next · Right: previous · Middle: reset','muted'));const value=q(id.replace('-cycle','')).value;tip.append(node('div',value?LEVEL_INFO[value].label:'Filter: off'));}tip.hidden=false;};
    button.onpointerenter=show;button.onfocus=show;button.onpointerleave=()=>q('bar-tooltip').hidden=true;button.onblur=()=>q('bar-tooltip').hidden=true;
  }
  function syncHotbar(){
    q('numbers').setAttribute('aria-pressed',String(showNumbers));
    const logarithmic=q('scale').value==='log';
    q('scale-toggle').setAttribute('aria-pressed',String(logarithmic));
    q('scale-toggle').setAttribute('aria-label',`Heat scale: ${logarithmic?'logarithmic':'linear'}. Switch to ${logarithmic?'linear':'logarithmic'}`);
    q('scale-toggle').querySelector('.badge').textContent=logarithmic?'LOG':'LIN';
q('resources').setAttribute('aria-pressed',q('color').value==='resources');for(const id of ['defense','treasury']){const value=q(id).value;const b=q(id+'-cycle');b.setAttribute('aria-pressed',!!value);b.querySelector('.badge').textContent=value?({VERY_LOW:'VL',LOW:'L',MEDIUM:'M',HIGH:'H',VERY_HIGH:'VH',NONE:'N'}[value]):'';}}
  const engine = new MapEngine(root.querySelector("canvas"), { onHover: (t) => {
    hovered = t;
    if (!locked) details(t);
  }, onClick: (t) => {
    locked = t;
    details(t);
  }, onCursorMove: (x, z) => {
    root.querySelector(".coords").textContent = x == null ? "" : `${Math.round(x)} ${Math.round(z)}`;
  } });
  engine.spawnMode = ingredient;
  const tower = createTowerCalculator();
  function details(t) {
    tower.select(current && !timeline && locked ? t : null, all);
    const box = q('details'); box.replaceChildren(); box.hidden = !t;
    if (!t) return;
    const top = node('div', null, 'tooltip-top');
    const center = node('div', null, 'tooltip-center');
    const namebox = node('div', null, 'tooltip-name');
    namebox.append(node('span', t.name));
    if (locked) { const unpin=node('button','×','unpin');unpin.setAttribute('aria-label','Unpin details');unpin.onclick=()=>{locked=null;details(null);};namebox.append(unpin); }
    const line = (label, value, color='#aaaaaa') => {const row=node('div',null,'detail-line');if(label)row.append(node('span',label));const v=node('span',value);v.style.color=color;row.append(v);center.append(row);};
    const spacer=()=>center.append(node('div',null,'spacer'));
    if(t.spawn){
      line('',t.mob,'#ff0083');spacer();line('Ingredient tier: ', t.tier??'Unknown','#ffff55');
      line('Average price: ',t.price==null?'Unknown':(t.price/64).toFixed(2)+' EB','#55ff55');
      line('Spawn radius: ',t.radius+' blocks','#55ffff');
    } else {
      line('',timeline && !t.historyKnown ? 'Ownership unknown at this time' : t.guildName ? `${t.guildName} [${t.ownerPrefix??t.prefix}]`:'Unclaimed','#ff0083');
      spacer();
      if(heat){line('Territory captures: ',(data.counts[t.name]??0).toLocaleString(),'#ffaa00');line('',data.timeframe,'#aaaaaa');}
      else {
        for(const key of RESOURCE_ORDER){const r=t.resources[key],meta=RESOURCES[key];if(!r)continue;
          for(const text of [r.generation?`+${r.generation} ${meta.name} per Hour`:null,r.stored === 0 ? null : `${r.stored??'?'}/${r.limit??'?'} ${meta.name} stored`].filter(Boolean)){
            const row=node('div',null,'detail-line');row.style.color=meta.color;
            if(meta.icon){const img=node('img');img.src='/map/assets'+meta.icon;img.alt='';img.className='res-icon';row.append(img);}row.append(document.createTextNode(text));center.append(row);
          }
        }
        if (timeline) { spacer();line('', 'Resources / treasury: not archived'); }
        else { spacer();line('✦ Treasury: ',LEVEL_INFO[t.treasury]?.label??'Unknown',LEVEL_INFO[t.treasury]?.treasury??'#ffffff'); }
        line('Territory Defences: ',LEVEL_INFO[t.defences]?.label??'Unknown',LEVEL_INFO[t.defences]?.defence??'#ffffff');
        if(timeline && t.hq == null) line('Headquarters: ', 'Unknown');
        if(t.hq){spacer();line('','Guild Headquarters','#ff5555');}
        spacer();line('Time Held: ',formatTimeHeld(t.acquired, engine.timeRef ?? Date.now()),'#ffff55');
      }
    }
    if (ingredient) { spacer();line('',`${Math.round(t.cx)}, ${Math.round(t.cz)}`,'#aaaaaa'); }
    box.append(top,center,namebox);
    if (current && !timeline && locked) box.append(tower.element);
  }
  function focus(t) {
    locked = t;
    setDrawer(false);
    details(t);
    engine.setView(t.cx, t.cz, Math.min(2, Math.max(0.3, Math.min(root.clientWidth * 0.5 / Math.max(50, t.x2 - t.x1), root.clientHeight * 0.5 / Math.max(50, t.z2 - t.z1)))));
  }
  function fit() {
    if (!shown.length) {
      engine.centerOnWorld();
      return;
    }
    const x1 = Math.min(...shown.map((t) => t.x1)), x2 = Math.max(...shown.map((t) => t.x2)), z1 = Math.min(...shown.map((t) => t.z1)), z2 = Math.max(...shown.map((t) => t.z2));
    engine.setView((x1 + x2) / 2, (z1 + z2) / 2, Math.min(root.clientWidth / (x2 - x1 + 400), root.clientHeight / (z2 - z1 + 400)) * 0.8);
  }
  function update() {
    const term = q("search").value.trim().toLowerCase();
    if (ingredient) {
      all = [];
      for (const i of data.ingredients) {
        if (q("tier").value !== "" && i.tier !== Number(q("tier").value)) continue;
        if (q("price").value !== "" && (i.price == null || i.price < Number(q("price").value) * 64)) continue;
        for (const s of i.spawns) {
          if (term && !`${i.name} ${s.mob}`.toLowerCase().includes(term)) continue;
          all.push({ ...s, ...i, spawns: void 0, spawn: true, cx: s.x, cz: s.z, x1: s.x - s.radius, x2: s.x + s.radius, z1: s.z - s.radius, z2: s.z + s.radius, links: [], guildColor: colorForStringHash(i.name) });
        }
      }
      shown = all;
    } else {
      all = territories.map(t => ({ ...t }));
      const max = Math.max(1, ...Object.values(data.counts ?? {}));
      for (const t of all) {
        if (timeline && !t.historyKnown) t.guildColor = '#7f7f7f';
        t.ownerPrefix = t.prefix;
        if (heat) {
          const count = data.counts[t.name] ?? 0, fraction = q("scale").value === "log" ? Math.log1p(count) / Math.log1p(max) : count / max;
          t.guildColor = hsl(220 - 220 * fraction, count ? 78 : 20, count ? 58 : 28);
          t.prefix = showNumbers ? String(count) : "";
          t.hq = false;
          t.acquired = null;
        } else if (["defense", "treasury"].includes(q("color").value)) {
          const type = q("color").value;
          t.guildColor = LEVEL_INFO[type === "defense" ? t.defences : t.treasury]?.[type === "defense" ? "defence" : "treasury"] ?? "#aaaaaa";
        }
      }
      shown = all.filter((t) => (current || !term || `${t.name} ${t.guildName ?? ""} ${t.ownerPrefix}`.toLowerCase().includes(term)) && (!current || !q("guild").value || t.guildName === q("guild").value) && (!current || !q("defense").value || t.defences === q("defense").value) && (!current || !q("treasury").value || t.treasury === q("treasury").value) && (!heat || (data.counts[t.name] ?? 0) >= Number(q("min").value)));
      engine.setResourceMode(current && q("color").value === "resources");
      if (heat) {
        const logarithmic = q('scale').value === 'log';
        const legend = root.querySelector('.legend');
        legend.replaceChildren();
        for (let i = 0; i <= 8; i++) {
          const swatch = node('span');
          swatch.style.background = i === 0 ? hsl(220, 20, 28) : hsl(220 - 220 * i / 8, 78, 58);
          legend.append(swatch);
        }
        q('heat-max').textContent = max.toLocaleString();
        q('legend').textContent = logarithmic ? 'Logarithmic scale' : 'Linear scale';
      }
    }
    engine.setTerritories(shown);
    syncHotbar();
    if (locked) {
      locked = shown.find((t) => t.name === locked.name && t.cx === locked.cx && t.cz === locked.cz) ?? null;
      details(locked);
    }
    const results = q("results");
    results.replaceChildren();
    q("summary").textContent = ingredient ? `${shown.length.toLocaleString()} spawn circles / ${new Set(shown.map((t) => t.name)).size} ingredients` : `${shown.length} territories${heat ? " / " + shown.reduce((s, t) => s + (data.counts[t.name] ?? 0), 0).toLocaleString() + " captures" : ""}`;
    if (!shown.length) {
      results.append(node("div", "No matches. Try clearing the filters.", "empty"));
      return;
    }
    const list = heat ? [...shown].sort((a, b) => (data.counts[b.name] ?? 0) - (data.counts[a.name] ?? 0)) : current && term ? shown.filter(t=>`${t.name} ${t.guildName??""} ${t.ownerPrefix}`.toLowerCase().includes(term)) : shown;
    if(!list.length) results.append(node("div","No matches.","empty"));
    for (const t of list.slice(0, 100)) {
      const b = node("button", (t.hq ? "\u265B " : "") + t.name, "entry");
      b.append(node("small", ingredient ? `${t.mob} / ${Math.round(t.cx)}, ${Math.round(t.cz)}` : heat ? `${data.counts[t.name] ?? 0} captures` : timeline && !t.historyKnown ? 'Unknown ownership' : t.guildName ?? "Unowned"));
      b.onclick = () => focus(t);
      results.append(b);
    }
    if (list.length > 100) results.append(node("p", `Showing first 100 of ${list.length}. Search to narrow the list.`, "muted"));
  }
  function guildOptions() {
    territories = processTerritories(raw, colors);
    const selected = q("guild").value;
    q("guild").replaceChildren(new Option("All guilds", ""));
    const counts = new Map();
    for (const t of territories) if (t.guildName) counts.set(t.guildName, (counts.get(t.guildName) ?? 0) + 1);
    const ranked = [...counts].sort((a, b) => b[1] - a[1]);
    for (const [name, count] of ranked) q("guild").append(new Option(`${name} / ${count}`, name));
    if (counts.has(selected)) q("guild").value = selected;
    q('guild-toggle').textContent=`Guilds · ${counts.size} ▾`;
    const list=q('guild-list');list.replaceChildren();
    const clear=node('button','Show all guilds','entry clear');clear.onclick=()=>{q('guild').value='';update();};list.append(clear);
    for(const [i,[name,count]] of ranked.entries()){
      const b=node('button',null,'guild-row');const swatch=node('span',null,'swatch');swatch.style.background=colors[name]?.color??colorForStringHash(name);
      b.append(node('span',String(i+1),'rank'),swatch,node('span',name,'guild-name'),node('span',String(count),'count'));
      b.onclick=()=>{q('guild').value=name;q('search').value='';update();fit();};list.append(b);
    }
  }
  for (const id of ["search", "color", "defense", "treasury", "guild", "tier", "price", "scale", "min"]) q(id).addEventListener("input", update);
  q("home").onclick = fit;
  q("minus").onclick = () => engine.setView(engine.camX, engine.camZ, engine.zoom / 1.5);
  q("plus").onclick = () => engine.setView(engine.camX, engine.camZ, engine.zoom * 1.5);
  q("timers").onclick = () => {
    engine.setTimersVisible(!engine.showTimers);
    q("timers").setAttribute("aria-pressed", engine.showTimers);
  };
  function setDrawer(open) {
    open = open && (ingredient || timeline);
    root.querySelector('aside').hidden=!open;
    root.querySelector('.toggle-panel').setAttribute('aria-expanded',open);
    q('settings').setAttribute('aria-pressed',open);
    if(open){q('guild-list').hidden=true;q('guild-toggle').setAttribute('aria-expanded','false');}
  }
  root.querySelector('.toggle-panel').onclick=()=>setDrawer(root.querySelector('aside').hidden);
  q('close').onclick=()=>setDrawer(false);
  q('search-open').onclick=()=>{setDrawer(true);q('search').focus();};
  q('numbers').onclick=()=>{showNumbers=!showNumbers;update();};
  q('scale-toggle').onclick=()=>{q('scale').value=q('scale').value==='log'?'linear':'log';update();q('scale-toggle').onpointerenter();};
  q('settings').onclick=()=>setDrawer(root.querySelector('aside').hidden);
  q('help').onclick=()=>{q('guide').hidden=!q('guide').hidden;q('help').setAttribute('aria-pressed',!q('guide').hidden);};
  q('resources').onclick=()=>{q('color').value=q('color').value==='resources'?'guild':'resources';update();};
  function cycleFilter(id, direction) {const select=q(id);const choices=['',...LEVELS];select.value=choices[(choices.indexOf(select.value)+direction+choices.length)%choices.length];update();}
  for(const id of ['defense','treasury']){
    const button=q(id+'-cycle');button.onclick=()=>cycleFilter(id,1);
    button.oncontextmenu=e=>{e.preventDefault();cycleFilter(id,-1);};
    button.onauxclick=e=>{if(e.button===1){e.preventDefault();q(id).value='';update();}};
  }
  q('guild-toggle').onclick=()=>{q('guild-list').hidden=!q('guild-list').hidden;q('guild-toggle').setAttribute('aria-expanded',!q('guild-list').hidden);if(!q('guild-list').hidden)setDrawer(false);};
  const keyHandler=e=>{
    if(e.key==='Escape'){locked=null;details(null);setDrawer(false);q('guild-list').hidden=true;q('guild-toggle').setAttribute('aria-expanded','false');q('guide').hidden=true;return;}
    if(e.target.closest('input,select'))return;
    if(e.key==='/'&&(ingredient||timeline)){e.preventDefault();q('search-open').click();}
    if(e.key.toLowerCase()==='g'&&current)q('guild-toggle').click();
    if(e.key.toLowerCase()==='h'&&current)q('timers').click();
    if(e.key==='?')q('help').click();
  };
  window.addEventListener('keydown',keyHandler,{signal:abort.signal});
  cleanup = () => {
    disposed = true;
    abort.abort();
    clearTimeout(timer);
    engine.destroy();
    root.remove();
    if (graphMain) graphMain.hidden = false;
  };
  window.addEventListener("pagehide", closeMap, { once: true, signal: abort.signal });
  guildOptions();
  update();
  if (ingredient) fit();
  else engine.centerOnWorld();
  const status = () => {
    q("status").classList.remove("status-error");
    q("status").textContent = timeline ? 'Historical snapshot · UTC' : current ? "● Live" : `Snapshot / ${new Date(data.generated_at * 1e3).toLocaleString()}`;
  };
  if (timeline) {
    engine.setTimeReference(data.timeline.initial ?? data.timeline.end);
    const playback = createTimeline(data, (frame, at) => {
      if (disposed) return;
      raw = frame;
      engine.setTimeReference(at);
      guildOptions();
      update();
      if (!locked && hovered) {
        hovered = shown.find(t => t.name === hovered.name) ?? null;
        details(hovered);
      }
      engine.dirty = true;
      status();
    }, abort.signal);
    root.append(playback.element);
  }
  async function refresh() {
    if (disposed) return;
    if (Date.now() / 1e3 >= data.expires_at) {
      q("status").textContent = "Session expired. Run /map current again";
      return;
    }
    if (!document.hidden) {
      try {
        if (!colorsLoaded) {
          const response = await fetch("/api/map/colors?v=3", { signal: abort.signal, cache: "no-store" });
          if (!response.ok) throw Error("Guild colors unavailable");
          const palette = await response.json();
          if (!palette || !Object.values(palette).some(g => /^#[0-9a-f]{6}$/i.test(g?.color))) {
            throw Error("Guild colors unavailable");
          }
          if (disposed) return;
          colors = palette;
          colorsLoaded = true;
          guildOptions();
          update();
        }
        const live = await json("/api/map/live", abort.signal);
        if (disposed) return;
        raw = live.territories;
        guildOptions();
        update();
        status();
        q("status").textContent += " · " + new Date(live.fetched_at).toLocaleTimeString();
      } catch {
        if (!disposed) {
          q("status").textContent = colorsLoaded
            ? "Update failed / showing last successful map / retrying"
            : "Athena colors unavailable / temporary colors shown / retrying";
          q("status").classList.add("status-error");
        }
      }
    }
    if (!disposed) timer = setTimeout(refresh, 3e4);
  }
  try {
    const [map, crown] = await Promise.all([loadImage("/map/assets/main-map.webp"), loadImage("/map/assets/crown.png")]);
    if (disposed) return;
    engine.setMapImage(map);
    engine.setCrownImage(crown);
    await document.fonts.load("15px Minecraft");
    engine.dirty = true;
    status();
    if (current && !timeline) {
      q("status").textContent = "Loading Athena guild colors…";
      refresh();
    }
  } catch {
    if (!disposed) {
      q("status").textContent = "Base map could not load. Check Worker assets and reopen the map.";
      q("status").classList.add("status-error");
    }
  }
}
function hsl(h, s, l) {
  s /= 100;
  l /= 100;
  const a = s * Math.min(l, 1 - l), f = (n) => {
    const k = (n + h / 30) % 12;
    return Math.round(255 * (l - a * Math.max(-1, Math.min(k - 3, 9 - k, 1)))).toString(16).padStart(2, "0");
  };
  return "#" + f(0) + f(8) + f(4);
}
export {
  closeMap,
  showMap
};
