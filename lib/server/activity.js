const element = (id) => document.getElementById(id);
const themes = {
  light: {
    background: '#ffffff', panel: '#f4f5f7', border: '#d5d9df', grid: '#e1e4e8',
    text: '#20242b', muted: '#596273', accent: '#397f32', accentRGB: '57,127,50',
  },
  dark: {
    background: '#121212', panel: '#1e1e1e', border: '#444444', grid: '#333333',
    text: '#f2f2f2', muted: '#aaaaaa', accent: '#73bf69', accentRGB: '115,191,105',
  },
  discord: {
    background: '#323339', panel: '#2b2d31', border: '#7289da', grid: '#4b4d55',
    text: '#f2f3f5', muted: '#c4c5c9', accent: '#73bf69', accentRGB: '115,191,105',
  },
};
let colors = themes.dark;
let selectedTheme;

function areaGradient() {
  return new echarts.graphic.LinearGradient(0, 0, 0, 1, [
    { offset: 0, color: `rgba(${colors.accentRGB},0.23)` },
    { offset: 1, color: `rgba(${colors.accentRGB},0.01)` },
  ]);
}

function applyTheme(theme) {
  theme = Object.hasOwn(themes, theme) ? theme : 'light';
  colors = themes[theme];
  element('chart-theme').value = theme;
  document.documentElement.style.colorScheme = theme === 'light' ? 'light' : 'dark';
  for (const key of ['background', 'panel', 'border', 'text', 'muted', 'accent']) {
    document.documentElement.style.setProperty(`--${key}`, colors[key]);
  }
  if (!chart) return;
  const option = {
    textStyle: { color: isTimeSeries() ? colors.muted : colors.text },
    tooltip: {
      backgroundColor: colors.panel, borderColor: colors.border,
      textStyle: { color: colors.text },
    },
  };
  if (isTimeSeries()) {
    option.tooltip.axisPointer = {
      lineStyle: { color: colors.muted }, crossStyle: { color: colors.muted },
      label: { backgroundColor: colors.panel, color: colors.text },
    };
    const axis = () => ({
      axisLabel: { color: colors.muted }, splitLine: { lineStyle: { color: colors.grid } },
    });
    option.xAxis = axis();
    option.yAxis = axis();
    option.dataZoom = [{
      id: 'slider', borderColor: colors.border, backgroundColor: colors.background,
      fillerColor: `rgba(${colors.accentRGB},0.12)`,
      handleStyle: { color: colors.panel, borderColor: colors.accent },
      moveHandleStyle: { color: colors.accent },
      textStyle: { color: colors.text },
      dataBackground: { lineStyle: { color: colors.muted }, areaStyle: { color: colors.muted } },
      selectedDataBackground: { lineStyle: { color: colors.accent }, areaStyle: { color: colors.accent } },
    }];
    option.series = [{
      id: 'online', lineStyle: { color: colors.accent }, itemStyle: { color: colors.accent },
      ...(graph.chart_type === 'bar' ? {} : { areaStyle: { color: areaGradient() } }),
    }];
  } else {
    option.legend = {
      textStyle: { color: colors.text }, pageTextStyle: { color: colors.muted },
      pageIconColor: colors.accent,
    };
    option.series = [{
      id: 'distribution', itemStyle: { borderColor: colors.background },
      label: { color: colors.text }, labelLine: { lineStyle: { color: colors.muted } },
    }];
  }
  chart.setOption(option, { silent: true });
  chart.dispatchAction({ type: 'hideTip' });
}

const dateFormatter = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'UTC',
  month: 'short',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23',
});

const generatedFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: 'UTC',
  month: '2-digit',
  day: '2-digit',
  year: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
  hour12: true,
});

let chart;
let graph;
let currentGraphId;
let instanceId;
let pollTimer;
let pendingRequests = 0;
let closed = false;
let selectionEnabled = true;
let currentRange = { start: 0, end: 100 };
let rangeHistory = [];
let gestureBefore;
let gestureTimer;
let applyingUndo = false;

function isTimeSeries() {
  return graph && graph.chart_type !== 'pie';
}

function isCategoryPie() {
  return graph?.chart_type === 'pie' && graph.pie_mode === 'categories';
}

function metricName() {
  return graph?.metric || 'Online members';
}

function summaryNumber(value) {
  return value.toLocaleString(undefined, {
    notation: Math.abs(value) >= 1000000 ? 'compact' : 'standard', maximumFractionDigits: 1,
  });
}

function showStatus(message) {
  element('status').textContent = message;
  element('status').hidden = false;
}

async function fetchJSON(path) {
  const response = await fetch(path, {
    cache: 'no-store',
    signal: AbortSignal.timeout(15000),
  });
  if (!response.ok) {
    const message = response.headers.get('Content-Type')?.includes('application/json')
      ? (await response.json()).error : await response.text();
    throw new Error(message || `Could not load the graph (HTTP ${response.status}).`);
  }
  return response.json();
}

async function loadECharts() {
  if (window.echarts) return;
  await new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = '/vendor/echarts.js';
    script.onload = resolve;
    script.onerror = () => reject(new Error('Could not load ECharts.'));
    document.head.appendChild(script);
  });
}

function updateStatistics(start, end) {
  element('last-label').textContent = isTimeSeries() ? 'Last' : 'Total';
  for (const statistic of document.querySelectorAll('.time-series-stat')) {
    statistic.hidden = !isTimeSeries();
  }
  if (!isTimeSeries()) {
    const values = graph.distribution.map((slice) => slice.value);
    const total = values.reduce((sum, value) => sum + value, 0);
    element('last').textContent = summaryNumber(total);
    return;
  }
  let count = 0;
  let total = 0;
  let last;
  let minimum = Infinity;
  let maximum = -Infinity;

  for (const point of graph.points) {
    if (point.x < start || point.x > end || point.y === null) continue;
    count++;
    total += point.y;
    last = point.y;
    minimum = Math.min(minimum, point.y);
    maximum = Math.max(maximum, point.y);
  }

  const values = {
    last: count ? summaryNumber(last) : '—',
    minimum: count ? summaryNumber(minimum) : '—',
    maximum: count ? summaryNumber(maximum) : '—',
    average: count ? summaryNumber(total / count) : '—',
  };
  for (const [id, value] of Object.entries(values)) element(id).textContent = value;
}

function formatTooltip(items) {
  const point = items.find((item) => item.seriesId === 'online');
  if (!point) return '';

  const tooltip = document.createElement('div');
  const timestamp = document.createElement('div');
  timestamp.textContent = `${dateFormatter.format(point.value[0])} UTC`;
  timestamp.style.cssText = `color:${colors.muted};font-size:12px;margin-bottom:10px`;

  const row = document.createElement('div');
  row.textContent = metricName();
  const value = document.createElement('strong');
  value.textContent = point.value[1] === null ? 'No sample' : point.value[1].toLocaleString();
  value.style.cssText = 'margin-left:28px;font:16px Consolas,monospace';
  row.appendChild(value);
  tooltip.append(timestamp, row);
  return tooltip;
}

function sameRange(a, b) {
  return Math.abs(a.start - b.start) < 0.000001
    && Math.abs(a.end - b.end) < 0.000001;
}

function updateUndoButton() {
  const pendingChange = gestureBefore && !sameRange(gestureBefore, currentRange);
  element('undo-range').disabled = !rangeHistory.length && !pendingChange;
}

function rememberRange(range) {
  rangeHistory.push({ ...range });
  if (rangeHistory.length > 100) rangeHistory.shift();
}

function finishGesture() {
  clearTimeout(gestureTimer);
  if (gestureBefore && !sameRange(gestureBefore, currentRange)) rememberRange(gestureBefore);
  gestureBefore = undefined;
  updateUndoButton();
}

function finishGestureSoon(delay = 180) {
  clearTimeout(gestureTimer);
  gestureTimer = setTimeout(finishGesture, delay);
}

function handleRangeChange() {
  const zoom = chart.getOption().dataZoom[0];
  const nextRange = { start: zoom.start, end: zoom.end };
  if (!sameRange(currentRange, nextRange)) {
    if (!applyingUndo && !gestureBefore) rememberRange(currentRange);
    currentRange = nextRange;
  }
  const duration = graph.end - graph.start;
  updateStatistics(
    graph.start + duration * currentRange.start / 100,
    graph.start + duration * currentRange.end / 100,
  );
  updateUndoButton();
}

function undoRange() {
  if (!chart || !isTimeSeries()) return;
  finishGesture();
  const previous = rangeHistory.pop();
  if (!previous) return;
  applyingUndo = true;
  try {
    chart.dispatchAction({ type: 'dataZoom', ...previous });
  } finally {
    applyingUndo = false;
  }
  updateUndoButton();
}

function resetRange() {
  if (!chart || !isTimeSeries()) return;
  finishGesture();
  chart.dispatchAction({ type: 'dataZoom', start: 0, end: 100 });
}

function setSelection(enabled) {
  if (!chart || !isTimeSeries()) return;
  chart.dispatchAction({
    type: 'takeGlobalCursor', key: 'dataZoomSelect', dataZoomSelectActive: enabled,
  });
}

function renderPie() {
  const palette = ['#73bf69', '#5bb6a2', '#5794d8', '#a48bdb', '#dfb665', '#de8a70', '#b7c5d3'];
  chart.setOption({
    animation: false,
    color: palette,
    textStyle: { fontFamily: 'Segoe UI, sans-serif', color: colors.text },
    tooltip: {
      trigger: 'item',
      confine: true,
      backgroundColor: colors.panel,
      borderColor: colors.border,
      textStyle: { color: colors.text, fontSize: 14 },
      formatter: (slice) => {
        const tooltip = document.createElement('div');
        const label = document.createElement('div');
        label.textContent = slice.name;
        label.style.marginBottom = '8px';
        const value = document.createElement('strong');
        value.textContent = isCategoryPie()
          ? `${slice.value.toLocaleString()} · ${slice.percent}%`
          : `${slice.value.toLocaleString()} samples · ${slice.percent}%`;
        tooltip.append(label, value);
        return tooltip;
      },
    },
    legend: {
      type: 'scroll',
      bottom: 12,
      left: 'center',
      width: '90%',
      selectedMode: false,
      textStyle: { color: colors.text, fontSize: 13 },
      pageTextStyle: { color: colors.muted },
      pageIconColor: colors.accent,
    },
    series: [{
      id: 'distribution',
      type: 'pie',
      name: isCategoryPie() ? metricName() : 'Recorded samples',
      radius: '65%',
      center: ['50%', '45%'],
      selectedMode: 'single',
      selectedOffset: 8,
      stillShowZeroSum: false,
      minShowLabelAngle: 4,
      data: graph.distribution,
      itemStyle: { borderColor: colors.background, borderWidth: 2 },
      label: {
        color: colors.text,
        fontSize: 13,
        formatter: '{b}\n{d}%',
        lineHeight: 19,
        alignTo: 'edge',
        edgeDistance: '4%',
      },
      labelLine: { lineStyle: { color: colors.muted } },
      emphasis: { scaleSize: 6 },
    }],
  });
  updateStatistics(graph.start, graph.end);
}

function renderGraph() {
  if (chart) chart.dispose();
  clearTimeout(gestureTimer);
  gestureBefore = undefined;
  rangeHistory = [];
  currentRange = { start: 0, end: 100 };
  chart = echarts.init(element('chart'), null, { renderer: 'canvas' });
  element('chart-controls').hidden = !isTimeSeries();
  document.querySelector('.summary').setAttribute('aria-label', isTimeSeries()
    ? 'Statistics for the visible time range' : 'Statistics for all pie categories');
  if (!isTimeSeries()) {
    element('chart').setAttribute('aria-label', isCategoryPie()
      ? `${metricName()} by category. Hover or tap a slice for its count and percentage.`
      : 'Hover or tap a slice for its sample count and percentage. Missing counts are excluded.');
    renderPie();
    return;
  }
  element('chart').setAttribute('aria-label', `${metricName()} history. Drag to select a time range, scroll to zoom, or use the overview slider. Press Ctrl+Z to undo navigation. Times are UTC.`);
  const minimumSpan = Math.min(60000, graph.end - graph.start);
  const isBar = graph.chart_type === 'bar';

  chart.setOption({
    animation: false,
    useUTC: true,
    textStyle: { fontFamily: 'Segoe UI, sans-serif', color: colors.muted },
    grid: { left: 12, right: 18, top: 15, bottom: 88, containLabel: true },
    tooltip: {
      trigger: 'axis',
      confine: true,
      backgroundColor: colors.panel,
      borderColor: colors.border,
      padding: 13,
      textStyle: { color: colors.text, fontSize: 14 },
      formatter: formatTooltip,
      axisPointer: {
        type: 'cross',
        lineStyle: { color: colors.muted, type: 'dashed' },
        crossStyle: { color: colors.muted },
        label: {
          backgroundColor: colors.panel,
          color: colors.text,
          fontSize: 11,
          formatter: (axis) => axis.axisDimension === 'x'
            ? `${dateFormatter.format(axis.value)} UTC`
            : Math.round(axis.value),
        },
      },
    },
    toolbox: {
      left: -100,
      top: -100,
      itemSize: 0,
      itemGap: 0,
      showTitle: false,
      feature: {
        dataZoom: { yAxisIndex: 'none' },
      },
    },
    xAxis: {
      type: 'time',
      min: graph.start,
      max: graph.end,
      splitNumber: 6,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: colors.muted, fontSize: 12, hideOverlap: true },
      splitLine: { show: true, lineStyle: { color: colors.grid } },
    },
    yAxis: {
      type: 'value',
      min: 0,
      minInterval: 1,
      splitNumber: 6,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: colors.muted, fontSize: 12,
        formatter: (value) => Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 }).format(value) },
      splitLine: { lineStyle: { color: colors.grid } },
    },
    dataZoom: [
      {
        id: 'inside',
        type: 'inside',
        xAxisIndex: 0,
        filterMode: 'none',
        minValueSpan: minimumSpan,
        zoomOnMouseWheel: true,
        moveOnMouseMove: false,
        moveOnMouseWheel: false,
        preventDefaultMouseMove: true,
      },
      {
        id: 'slider',
        type: 'slider',
        xAxisIndex: 0,
        filterMode: 'none',
        minValueSpan: minimumSpan,
        bottom: 14,
        height: 30,
        left: 40,
        right: 18,
        brushSelect: true,
        showDetail: false,
        borderColor: colors.border,
        backgroundColor: colors.background,
        fillerColor: `rgba(${colors.accentRGB},0.12)`,
        handleStyle: { color: colors.panel, borderColor: colors.accent },
        moveHandleStyle: { color: colors.accent },
        dataBackground: {
          lineStyle: { color: colors.muted, opacity: 0.5 },
          areaStyle: { color: colors.muted, opacity: 0.08 },
        },
        selectedDataBackground: {
          lineStyle: { color: colors.accent },
          areaStyle: { color: colors.accent, opacity: 0.16 },
        },
      },
    ],
    series: [{
      id: 'online',
      name: metricName(),
      type: isBar ? 'bar' : 'line',
      barMaxWidth: 36,
      barCategoryGap: '20%',
      showSymbol: false,
      symbol: 'circle',
      symbolSize: 6,
      connectNulls: false,
      clip: true,
      data: graph.points.map((point) => [point.x, point.y]),
      lineStyle: { color: colors.accent, width: 1.6 },
      itemStyle: { color: colors.accent },
      areaStyle: isBar ? undefined : {
        color: areaGradient(),
      },
      emphasis: { lineStyle: { width: 1.6 } },
    }],
  });

  chart.on('datazoom', handleRangeChange);

  chart.on('globalcursortaken', (event) => {
    if (event.key !== 'dataZoomSelect') return;
    selectionEnabled = event.dataZoomSelectActive;
    element('select-range').setAttribute('aria-pressed', String(selectionEnabled));
    chart.setOption({
      dataZoom: [{ id: 'inside', moveOnMouseMove: !selectionEnabled }],
    }, { silent: true });
  });

  setSelection(true);
  element('select-range').disabled = false;
  element('reset-range').disabled = false;
  updateUndoButton();
  updateStatistics(graph.start, graph.end);
}

async function showGraph(id) {
  graph = await fetchJSON(`/api/graphs/${encodeURIComponent(id)}`);
  currentGraphId = id;
  if (graph.chart_type === 'map') {
    chart?.dispose(); chart = null;
    const { showMap } = await import('/map/viewer.js');
    await showMap(graph);
    element('status').hidden = true;
    return;
  }
  if (document.getElementById('map-viewer')) {
    const { closeMap } = await import('/map/viewer.js');
    closeMap();
  }
  await loadECharts();
  const graphTitle = graph.chart_type === 'pie' ? 'Online Member Distribution'
    : graph.chart_type === 'bar' ? 'Online Members (Bar)' : 'Online Members';
  const title = graph.title || `${graphTitle} - ${graph.name}`;
  document.title = title;
  element('graph-title').textContent = title;
  element('metric-name').textContent = metricName();
  const generatedAt = graph.generated_at ?? graph.expires_at - 86400;
  element('generated-at').textContent = `Generated at ${generatedFormatter.format(generatedAt * 1000)} UTC.`;
  element('demo-label').hidden = !graph.demo;
  chart?.dispose();
  chart = null;
  applyTheme(selectedTheme ?? graph.theme);
  renderGraph();
  element('status').hidden = true;
}

async function pollInstance() {
  if (document.hidden && currentGraphId) {
    if (!closed) pollTimer = setTimeout(pollInstance, 15000);
    return;
  }
  try {
    const selection = await fetchJSON(`/api/instances/${encodeURIComponent(instanceId)}`);
    if (selection.graph_id !== currentGraphId) await showGraph(selection.graph_id);
    else element('status').hidden = true;
    pendingRequests = 0;
  } catch (error) {
    pendingRequests++;
    showStatus(pendingRequests < 10 && !currentGraphId
      ? 'Waiting for the bot to attach your graph…'
      : error.message);
  } finally {
    if (!closed) pollTimer = setTimeout(pollInstance,
      currentGraphId || pendingRequests > 10 ? 15000 : 2000);
  }
}

element('select-range').addEventListener('click', () => setSelection(!selectionEnabled));
element('undo-range').addEventListener('click', undoRange);
element('reset-range').addEventListener('click', resetRange);
element('chart-theme').addEventListener('change', (event) => {
  selectedTheme = event.target.value;
  applyTheme(selectedTheme);
});

element('chart').addEventListener('pointerdown', () => {
  if (!chart || !isTimeSeries()) return;
  finishGesture();
  gestureBefore = { ...currentRange };
}, { capture: true });

for (const event of ['pointerup', 'pointercancel']) {
  window.addEventListener(event, () => {
    if (gestureBefore) finishGestureSoon();
  }, { capture: true });
}

element('chart').addEventListener('wheel', () => {
  if (!chart || !isTimeSeries()) return;
  gestureBefore ??= { ...currentRange };
  finishGestureSoon(350);
}, { capture: true, passive: true });

window.addEventListener('keydown', (event) => {
  if (!(event.ctrlKey || event.metaKey) || event.shiftKey || event.altKey
      || event.key.toLowerCase() !== 'z') return;
  if (event.target instanceof Element
      && event.target.closest('input, textarea, select, [contenteditable="true"]')) return;
  if (!chart || !isTimeSeries()) return;
  event.preventDefault();
  if (!event.repeat) undoRange();
});

const resizeObserver = new ResizeObserver(() => chart?.resize());
resizeObserver.observe(element('chart'));
window.addEventListener('pagehide', () => {
  closed = true;
  clearTimeout(pollTimer);
  clearTimeout(gestureTimer);
  resizeObserver.disconnect();
  chart?.dispose();
});

try {
  const params = new URLSearchParams(location.search);

  if (params.has('frame_id')) {
    const config = await fetchJSON('/api/config');
    if (!config.application_id) {
      throw new Error('Run an activity command and click Explore graph to open this Activity.');
    }
    const { DiscordSDK } = await import('/vendor/discord-sdk.mjs');
    const sdk = new DiscordSDK(config.application_id);
    await Promise.race([
      sdk.ready(),
      new Promise((_, reject) => setTimeout(() => reject(
        new Error('Discord connection timed out. Check the application URL Mapping.'),
      ), 15000)),
    ]);
    instanceId = sdk.instanceId;
    if (!instanceId) throw new Error('Discord did not supply an Activity instance.');
    await pollInstance();
  } else if (params.has('graph')) {
    await showGraph(params.get('graph'));
  } else {
    showStatus('Run an activity command and click Explore graph to open this Activity.');
  }
} catch (error) {
  showStatus(error.message);
}
