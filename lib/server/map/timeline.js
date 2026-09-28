export function replayWindow(window, at) {
  const {timeline} = window;
  if (at<timeline.window_start || at>timeline.window_end) throw Error('Time is outside the loaded window');
  const raw = Object.fromEntries(Object.entries(window.territories).map(([name,t])=>[name,{...t}]));
  let changes=0;
  for (const [time,name,guild,acquired,hq,defense] of timeline.events) {
    if (time>at) break;
    const t=raw[name];
    if (!t) continue;
    if (guild>=0) {
      Object.assign(t,{guild:timeline.guilds[guild],acquired:new Date(acquired).toISOString(),hq:null,defences:null,history_known:true});
      changes++;
    } else if (t.history_known && Date.parse(t.acquired)===acquired) {
      t.hq=hq;
      t.defences=defense;
    }
  }
  return {raw,changes};
}

export function utcInput(ms) {
  return new Date(ms).toISOString().slice(0,19);
}

export function parseUTC(value) {
  if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d(?::\d\d)?$/.test(value)) return NaN;
  return Date.parse(value+'Z');
}

export function createWindowLoader(data, signal) {
  const cache=new Map(), pending=new Map(), hour=3600000;
  function remember(window){
    cache.delete(window.timeline.window_start);
    cache.set(window.timeline.window_start,window);
    while(cache.size>12)cache.delete(cache.keys().next().value);
    return window;
  }
  remember(data);
  function cached(at){
    for(const window of cache.values())if(at>=window.timeline.window_start&&at<=window.timeline.window_end)return remember(window);
  }
  function wait(ms,abortSignal){return new Promise((resolve,reject)=>{
    const abort=()=>{clearTimeout(timer);reject(new DOMException('Aborted','AbortError'));};
    const timer=setTimeout(()=>{abortSignal.removeEventListener('abort',abort);resolve();},ms);
    if(abortSignal.aborted)abort();else abortSignal.addEventListener('abort',abort,{once:true});
  });}
  async function fetchWindow(at,abortSignal){
    const deadline=Date.now()+110000;
    while(Date.now()<deadline){
      const response=await fetch(`/api/timeline/${encodeURIComponent(data.id)}?at=${at}&wait=10`,{signal:abortSignal,cache:'no-store'});
      const body=await response.json();
      if(response.status===202||response.status===429){await wait(250,abortSignal);continue;}
      if(!response.ok)throw Error(body.error||'Historical data could not load.');
      return remember(body);
    }
    throw Error('The bot/API did not respond. Keep this time selected and press Go to retry.');
  }
  function shared(at){
    const present=cached(at);
    if(present)return Promise.resolve(present);
    const key=Math.floor(at/hour);
    if(pending.has(key))return pending.get(key).promise;
    if(pending.size>=3){const [oldest,job]=pending.entries().next().value;job.controller.abort();pending.delete(oldest);}
    const controller=new AbortController();
    const promise=fetchWindow(at,controller.signal).finally(()=>{
      if(pending.get(key)?.controller===controller)pending.delete(key);
    });
    pending.set(key,{controller,promise});
    return promise;
  }
  function load(at,abortSignal=signal){
    if(signal.aborted||abortSignal.aborted)return Promise.reject(new DOMException('Aborted','AbortError'));
    const result=shared(at);
    return new Promise((resolve,reject)=>{
      const abort=()=>reject(new DOMException('Aborted','AbortError'));
      abortSignal.addEventListener('abort',abort,{once:true});
      result.then(resolve,reject).finally(()=>abortSignal.removeEventListener('abort',abort));
    });
  }
  function preload(times){
    for(const at of times){
      if(signal.aborted||at<data.timeline.start||at>data.timeline.end||cached(at))continue;
      load(at).catch(()=>{});
    }
  }
  signal.addEventListener('abort',()=>{
    for(const job of pending.values())job.controller.abort();
    pending.clear();cache.clear();
  },{once:true});
  return {load,preload};
}

export function createTimeline(data, onFrame, signal) {
  const {start,end}=data.timeline;
  const panel=document.createElement('section');
  panel.className='timeline-panel mc-panel';
  panel.setAttribute('aria-label','Historical map timeline');
  panel.innerHTML=`<div class="timeline-row"><strong>Historical map · UTC</strong><output class="timeline-time"></output></div>
    <input class="timeline-range" type="range" step="1000" aria-label="Archive timeline">
    <div class="timeline-row timeline-bounds"><span></span><span></span></div>
    <div class="timeline-row timeline-controls">
      <button type="button" data-action="first" aria-label="Start of archive">|◀</button>
      <button type="button" data-action="back" aria-label="Step backward">−</button>
      <button type="button" data-action="play" aria-label="Play timeline" aria-pressed="false">Play</button>
      <button type="button" data-action="forward" aria-label="Step forward">+</button>
      <button type="button" data-action="last" aria-label="Frozen archive end">▶|</button>
      <select class="timeline-speed" aria-label="Time advanced each second"><option value="60000">1 min / sec</option><option value="600000" selected>10 min / sec</option><option value="3600000">1 hour / sec</option><option value="21600000">6 hours / sec</option><option value="86400000">1 day / sec</option></select>
      <input class="timeline-date" type="datetime-local" step="1" aria-label="Exact UTC date and time">
      <button type="button" data-action="go">Go</button><button type="button" data-action="copy">Copy time link</button>
    </div><div class="timeline-feedback" role="status" aria-live="polite"></div>
    <details><summary>Coverage and missing data</summary><p>Ownership is reconstructed from recorded captures. HQ and defense appear only when observed. Missing observations and collector outages can hide changes; unknown ownership is gray. Resources and treasury are not archived. The base map, territory boundaries, links and guild names use the available definitions, not historical versions. No live ownership updates are used.</p><p class="timeline-coverage"></p></details>`;
  const q=selector=>panel.querySelector(selector), action=name=>q(`[data-action="${name}"]`);
  const slider=q('.timeline-range'), date=q('.timeline-date'), feedback=q('.timeline-feedback');
  slider.min=String(start);slider.max=String(end);date.min=utcInput(start);date.max=utcInput(end);
  const label=ms=>utcInput(ms).replace('T',' ')+' UTC';
  q('.timeline-bounds').children[0].textContent=label(start);
  q('.timeline-bounds').children[1].textContent=label(end);
  q('.timeline-coverage').textContent='The archive ends at '+label(end)+'. Uncached times require the bot/API online. Links expire one hour after creation.';
  const loader=createWindowLoader(data,signal);
  let current=data.timeline.initial ?? end, playing=false, timer, debounce, request, serial=0, destroyed=false;
  let currentWindow=data;
  const step=()=>Number(q('.timeline-speed').value);
  function preload(){
    const boundary=currentWindow.timeline.window_end;
    loader.preload(step()>3600000
      ? [Math.min(end,current+step()),Math.min(end,current+2*step())]
      : [boundary+1,boundary+3600001]);
  }
  function pause(){
    playing=false;clearTimeout(timer);request?.abort();serial++;
    action('play').textContent='Play';action('play').setAttribute('aria-pressed','false');action('play').setAttribute('aria-label','Play timeline');
    if(feedback.textContent==='Loading historical state…')feedback.textContent='Paused at '+label(current);
  }
  async function seek(at){
    if(!Number.isFinite(at)||at<start||at>end){feedback.textContent='Choose a UTC time inside the displayed archive bounds.';return false;}
    const version=++serial;
    request?.abort();request=new AbortController();
    feedback.textContent='Loading historical state…';
    try{
      const window=await loader.load(at,request.signal);
      if(destroyed||version!==serial)return false;
      const frame=replayWindow(window,at);current=at;currentWindow=window;
      onFrame(frame.raw,at);
      slider.value=String(at);date.value=utcInput(at);q('.timeline-time').textContent=label(at);
      const known=Object.values(frame.raw).filter(t=>t.history_known).length;
      feedback.textContent=`${known} / ${Object.keys(frame.raw).length} territories with recorded ownership · Frozen snapshot${window.timeline.missing_geometry ? ' · '+window.timeline.missing_geometry+' historical names have no map geometry' : ''}`;
      preload();
      return true;
    }catch(error){
      if(error.name==='AbortError'||destroyed||version!==serial)return false;
      pause();feedback.textContent=error.message;return false;
    }
  }
  async function tick(){
    if(!playing||destroyed)return;
    if(document.hidden){pause();return;}
    const next=Math.min(end,current+step());
    if(await seek(next)){
      if(next===end)pause();else if(playing)timer=setTimeout(tick,1000);
    }
  }
  function manual(at){pause();clearTimeout(debounce);return seek(at);}
  action('play').onclick=()=>{
    if(playing){pause();return;}
    if(current>=end){feedback.textContent='At the frozen end. Pick an earlier time or jump to the start to play.';return;}
    playing=true;preload();action('play').textContent='Pause';action('play').setAttribute('aria-pressed','true');action('play').setAttribute('aria-label','Pause timeline');timer=setTimeout(tick,1000);
  };
  q('.timeline-speed').onchange=preload;
  action('first').onclick=()=>manual(start);action('last').onclick=()=>manual(end);
  action('back').onclick=()=>manual(Math.max(start,current-step()));action('forward').onclick=()=>manual(Math.min(end,current+step()));
  const enteredTime=()=>{const value=parseUTC(date.value);return value===Math.floor(start/1000)*1000 ? start : value;};
  action('go').onclick=()=>manual(enteredTime());
  date.onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();manual(enteredTime());}};
  slider.oninput=()=>{pause();clearTimeout(debounce);request?.abort();serial++;const at=Number(slider.value);feedback.textContent='Seeking to '+label(at);debounce=setTimeout(()=>seek(at),180);};
  action('copy').onclick=async()=>{
    const url=new URL(location.href);url.search='';url.searchParams.set('graph',data.id);url.searchParams.set('at',String(current));
    try{await navigator.clipboard.writeText(url.href);feedback.textContent='Link copied at '+label(current);}catch{feedback.textContent='Could not copy the link in this browser.';}
  };
  const destroy=()=>{destroyed=true;pause();clearTimeout(debounce);request?.abort();};
  signal.addEventListener('abort',destroy,{once:true});
  document.addEventListener('visibilitychange',()=>{if(document.hidden)pause();},{signal});
  const linked=Number(new URLSearchParams(location.search).get('at'));
  if(new URLSearchParams(location.search).has('at')&&linked>=start&&linked<=end)current=linked;
  queueMicrotask(()=>{if(!destroyed)seek(current);});
  return {element:panel,destroy};
}
