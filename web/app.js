(() => {
'use strict';

/* ======================================================================
   Utilitaires
   ====================================================================== */
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
const NS = 'http://www.w3.org/2000/svg';
const LOW_N = 50; // en dessous : indicateurs jugés peu fiables
const nf0 = new Intl.NumberFormat('fr-FR', { maximumFractionDigits: 0 });
const nf1 = new Intl.NumberFormat('fr-FR', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const nf2 = new Intl.NumberFormat('fr-FR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ok = x => typeof x === 'number' && isFinite(x);
const fInt = x => ok(x) ? nf0.format(x) : '–';
const fPct = x => ok(x) ? nf1.format(x * 100) + ' %' : '–';
const fPct2 = x => ok(x) ? nf2.format(x * 100) + ' %' : '–';
const fIdx = x => ok(x) ? nf2.format(x) : '–';
const fDelta = x => ok(x) ? (x >= 0 ? '+' : '−') + nf2.format(Math.abs(x)) : '–';
const fGrowth = x => ok(x) ? (x >= 0 ? '+' : '−') + nf1.format(Math.abs(x * 100)) + ' %/an' : '–';
const fPts = x => ok(x) ? (x >= 0 ? '+' : '−') + nf1.format(Math.abs(x * 100)) + ' pt' : '–';
const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const enc = s => new TextEncoder().encode(s);
const b64d = s => Uint8Array.from(atob(s), c => c.charCodeAt(0));
const b64e = u8 => btoa(String.fromCharCode.apply(null, Array.from(u8)));
const range = (a, b) => { const r = []; for (let y = a; y <= b; y++) r.push(y); return r; };
const yrs = ys => !ys.length ? '–' : ys.length === 1 ? `${ys[0]}` : `${ys[0]}–${ys[ys.length - 1]}`;
const mean = a => { const v = a.filter(ok); return v.length ? v.reduce((s, x) => s + x, 0) / v.length : NaN; };

function el(tag, attrs = {}, parent) {
  const e = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) if (v !== undefined && v !== null) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
}
function txt(parent, x, y, s, attrs = {}) { const t = el('text', { x, y, ...attrs }, parent); t.textContent = s; return t; }
function truncate(s, n) { return s.length > n ? s.slice(0, n - 1).trimEnd() + '…' : s; }
function niceTicks(a, b, n) {
  const span = b - a; if (!(span > 0)) return [a];
  const step0 = span / n, mag = Math.pow(10, Math.floor(Math.log10(step0)));
  const step = [1, 2, 2.5, 5, 10].map(k => k * mag).find(s => s >= step0);
  const out = []; for (let v = Math.ceil(a / step) * step; v <= b + 1e-12; v += step) out.push(+v.toFixed(10));
  return out;
}

/* ======================================================================
   Chiffrement : AES-256-GCM, clé dérivée par PBKDF2-SHA256
   ====================================================================== */
const SESSION_KEY = 'tbx-session';
const payload = () => JSON.parse($('#payload').textContent);
async function sha256hex(bytes) {
  const h = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  return Array.from(h, b => b.toString(16).padStart(2, '0')).join('');
}
async function unwrapKey(login, pwd) {
  const P = payload();
  const gsalt = b64d(P.gsalt);
  const l = enc(':' + login.trim().toLowerCase());
  const buf = new Uint8Array(gsalt.length + l.length); buf.set(gsalt); buf.set(l, gsalt.length);
  const id = await sha256hex(buf);
  const u = P.users.find(x => x.uid === id);
  const base = await crypto.subtle.importKey('raw', enc(pwd), 'PBKDF2', false, ['deriveKey']);
  const kek = await crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt: u ? b64d(u.salt) : gsalt, iterations: P.iter, hash: 'SHA-256' },
    base, { name: 'AES-GCM', length: 256 }, false, ['decrypt']);
  if (!u) throw new Error('auth');
  try { return new Uint8Array(await crypto.subtle.decrypt({ name: 'AES-GCM', iv: b64d(u.iv) }, kek, b64d(u.wk))); }
  catch (e) { throw new Error('auth'); }
}
async function decryptData(dekRaw) {
  const P = payload();
  const dek = await crypto.subtle.importKey('raw', dekRaw, 'AES-GCM', false, ['decrypt']);
  const gz = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: b64d(P.iv) }, dek, b64d(P.ct));
  const stream = new Blob([gz]).stream().pipeThrough(new DecompressionStream('gzip'));
  return JSON.parse(await new Response(stream).text());
}
function sessionGet() { try { return JSON.parse(sessionStorage.getItem(SESSION_KEY) || 'null'); } catch (e) { return null; } }
function sessionSet(v) { try { v ? sessionStorage.setItem(SESSION_KEY, JSON.stringify(v)) : sessionStorage.removeItem(SESSION_KEY); } catch (e) { /* indisponible */ } }

/* ======================================================================
   Modèle
   Deux indicateurs, tous deux des PARTS MONDIALES, calculés sur les 38 axes disciplinaires.
   Une macro-discipline ou « Toutes disciplines » = somme des comptes des axes qui la composent.
   ====================================================================== */
let D = null, Y = [], CE = null;
const T = {}, E = {};
const PANEL = { eu: [], world: [], all: [] };
const LOW = 20;   // moins de 20 articles comptés : valeur peu fiable
const state = { disc: 'ALL', region: '', etab: '', y0: null, y1: null, intl: 't10', bar: 't10', mx: 't10', mxset: 'd', sortBars: false };

const IND = {
  t10: { k: 1, cit: true, label: 'Part du top 10 % mondial', short: 'Top 10 % mondial', theme: 'Influence',
         themeDef: 'être cité : poids dans les articles les plus cités',
         period: (ce, y1) => ce < y1 ? `Période arrêtée à ${ce} : les articles de ${ce + 1 === y1 ? y1 : `${ce + 1}–${y1}`} n’ont pas encore eu le temps d’être cités, leur classement dans le top 10 % ne serait pas stable.` : '',
         what: 'articles mondiaux du top 10 % des plus cités',
         whatDisc: d => d === 'ALL' ? 'des articles du monde entier qui figurent dans le top 10 % des plus cités de leur discipline' : 'des articles du monde entier qui figurent dans le top 10 % des plus cités de cette discipline',
         def: 'articles de l’entité figurant parmi les 10 % les plus cités au monde (même année, même sous-domaine) ÷ ensemble des articles mondiaux de ce top 10 %',
         ost: 'figure 6 du rapport OST (« influence »)' },
  tj:  { k: 2, cit: false, label: 'Part des revues de tête', short: 'Revues de tête', theme: 'Prestige',
         themeDef: 'publier dans les revues les plus réputées de sa discipline',
         period: (ce, y1) => ce < y1 ? `Période complète jusqu’à ${y1} : savoir qu’un article est paru dans une revue de tête ne demande pas d’attendre ses citations.` : '',
         what: 'articles mondiaux parus dans les revues de tête (10 revues SJR par axe)',
         whatDisc: d => isAxis(d) ? 'des articles du monde entier parus dans les 10 revues de tête de cette discipline' : 'des articles du monde entier parus dans les revues de tête de leur discipline (10 revues SJR par axe)',
         def: 'articles de l’entité parus dans les 10 meilleures revues SJR de leur discipline ÷ ensemble des articles mondiaux parus dans ces revues',
         ost: 'figure 7 du rapport OST (« prestige »), à une différence près : l’OST retient le décile des revues les plus citées' },
};

function indexData(data) {
  D = data; Y = data.meta.years; CE = Math.min(data.meta.citation_end_year || Y[Y.length - 1], Y[Y.length - 1]);
  T.macros = data.taxonomy.macros.filter(m => !m.transverse);
  T.macro = Object.fromEntries(T.macros.map(m => [m.code, m]));
  T.axes = data.taxonomy.axes.filter(a => T.macro[a.macro]);          // les 38 axes disciplinaires
  T.hm = data.taxonomy.macros.find(m => m.transverse) || { code: 'H', label: 'Axes transverses', short: 'Transverses' };
  T.taxes = data.taxonomy.axes.filter(a => a.macro === T.hm.code && D.cube.WORLD && D.cube.WORLD[a.code]);   // 19 axes transverses
  T.all = [...T.axes, ...T.taxes];
  T.axis = Object.fromEntries(T.all.map(a => [a.code, a]));
  T.axesOf = {}; T.axes.forEach(a => (T.axesOf[a.macro] = T.axesOf[a.macro] || []).push(a.code));
  data.entities.forEach(e => (E[e.id] = e));
  const pan = data.meta.panels || {};
  PANEL.eu = (pan.europe || []).filter(id => D.cube[id]);
  PANEL.world = (pan.monde || []).filter(id => D.cube[id]);
  PANEL.all = Array.from(new Set(['FR', ...PANEL.eu, ...PANEL.world])).filter(id => D.cube[id]);
}
const isMacro = c => !!T.macro[c];
const isAxis = c => !!T.axis[c];
const isTrans = c => isAxis(c) && T.axis[c].macro === T.hm.code;
const macroLabel = c => isTrans(c) ? T.hm.label : T.macro[macroOf(c)].label;
const macroOf = c => isAxis(c) ? T.axis[c].macro : (isMacro(c) ? c : null);
const colorOf = c => `var(--m-${macroOf(c) || 'H'})`;
const discLabel = c => c === 'ALL' ? 'Toutes disciplines (38 axes)' : isMacro(c) ? T.macro[c].label : T.axis[c].label;
const discName = c => c === 'ALL' ? 'Toutes disciplines' : isMacro(c) ? `${c} · ${T.macro[c].short}` : `${c} · ${T.axis[c].label}`;
const members = c => c === 'ALL' ? T.axes.map(a => a.code) : isMacro(c) ? (T.axesOf[c] || []) : [c];
const entityId = () => state.etab || (state.region && state.region !== '__NAT__' ? 'R:' + state.region : 'FR');
const isNational = () => entityId() === 'FR';
function affilText(eid) {
  const e = E[eid];
  if (eid === 'FR') return 'au moins un auteur affilié en France (métropole ou outre-mer)';
  if (e.type === 'region') return `au moins un auteur affilié à un établissement de la région ${e.label}`;
  return `au moins un auteur affilié à ${e.label}`;
}
const who = l => l === 'France' ? 'la France' : l;
const ofWho = l => l === 'France' ? 'de la France' : (/^[AEIOUYÉÈÎ]/i.test(l) ? 'd’' : 'de ') + l;

/* ---------- périodes ---------- */
function windows() {
  const all = range(state.y0, state.y1), cit = all.filter(y => y <= CE);
  // P1 = première moitié (l'année du milieu y est rattachée si le nombre d'années est impair), P2 = le reste.
  // Les indicateurs sont des parts (ratios) : des sous-périodes de durées un peu différentes restent comparables.
  const split = ys => { const h = Math.ceil(ys.length / 2); return ys.length >= 2 ? [ys.slice(0, h), ys.slice(h)] : [[], []]; };
  const [c1, c2] = split(cit), [a1, a2] = split(all);
  return { all, cit, c1, c2, a1, a2 };
}
const yearsOf = (ind, W) => IND[ind].cit ? { ys: W.cit, p1: W.c1, p2: W.c2 } : { ys: W.all, p1: W.a1, p2: W.a2 };

/* ---------- comptes : somme sur les axes de la discipline et sur les années ---------- */
function cnt(eid, disc, k, years) {
  const c = D.cube[eid]; if (!c) return NaN;
  let t = 0, any = false;
  for (const a of members(disc)) { const s = c[a]; if (!s) continue; any = true; for (const y of years) t += s[k][y - Y[0]] || 0; }
  return any ? t : NaN;
}
const share = (eid, disc, k, ys) => { const w = cnt('WORLD', disc, k, ys); return w > 0 ? cnt(eid, disc, k, ys) / w : NaN; };

/* ---------- un indicateur, pour une entité et une discipline ---------- */
function rankAmong(ids, id, f) {
  const v = ids.map(i => [i, f(i)]).filter(x => ok(x[1])).sort((a, b) => b[1] - a[1]);
  const k = v.findIndex(x => x[0] === id); return k < 0 ? null : { rank: k + 1, of: v.length };
}
function stat(eid, disc, ind, W = windows()) {
  const { ys, p1, p2 } = yearsOf(ind, W), k = IND[ind].k;
  const m = { disc, ind, ys, p1, p2, n: cnt(eid, disc, k, ys), w: cnt('WORLD', disc, k, ys) };
  m.s = m.w > 0 ? m.n / m.w : NaN;
  m.n1 = cnt(eid, disc, k, p1); m.w1 = cnt('WORLD', disc, k, p1); m.s1 = m.w1 > 0 ? m.n1 / m.w1 : NaN;
  m.n2 = cnt(eid, disc, k, p2); m.w2 = cnt('WORLD', disc, k, p2); m.s2 = m.w2 > 0 ? m.n2 / m.w2 : NaN;
  m.evo = m.s1 > 0 ? m.s2 / m.s1 - 1 : NaN;
  m.low = !(m.n >= LOW);
  if (eid === 'FR') {
    m.rk = rankAmong(PANEL.all, 'FR', id => cnt(id, disc, k, ys));
    m.rkEU = rankAmong(['FR', ...PANEL.eu], 'FR', id => cnt(id, disc, k, ys));
    m.rk1 = rankAmong(PANEL.all, 'FR', id => cnt(id, disc, k, p1));
    m.rk2 = rankAmong(PANEL.all, 'FR', id => cnt(id, disc, k, p2));
  } else {
    m.nFR = cnt('FR', disc, k, ys); m.sFR = m.nFR > 0 ? m.n / m.nFR : NaN;
  }
  return m;
}
const ordinal = r => r ? `${r.rank}<sup>${r.rank === 1 ? 'er' : 'e'}</sup>` : '–';
const rk = r => r ? `${ordinal(r)} sur ${r.of}` : '–';
const rkTxt = r => r ? `${r.rank}${r.rank === 1 ? 'er' : 'e'} sur ${r.of}` : '';
const fEvo = x => ok(x) ? (x >= 0 ? '+' : '−') + nf0.format(Math.abs(x * 100)) + ' %' : '–';

/* ======================================================================
   Textes
   ====================================================================== */
const TYPE_LABELS = { article: 'articles de recherche', review: 'articles de synthèse', 'conference-paper': 'communications de colloque',
  preprint: 'prépublications', book: 'ouvrages', 'book-chapter': 'chapitres d’ouvrage' };
function scopeText() {
  const types = D.meta.types || ['article', 'review'];
  const lbl = types.map(t => TYPE_LABELS[t] || t);
  const list = lbl.length > 1 ? lbl.slice(0, -1).join(', ') + ' et ' + lbl[lbl.length - 1] : lbl[0];
  return `<span class="fr-icon-information-line fr-icon--sm" aria-hidden="true"></span> « Articles » = ${list} indexés dans OpenAlex, compte entier.`
    + ` Deux indicateurs, calculés axe par axe : 38 axes disciplinaires, que les macro-disciplines et « toutes disciplines » additionnent, et 19 axes transverses, présentés à part car ils les recoupent.`
    + ` <a href="#acc-method" class="fr-link fr-link--sm" data-open-method>Méthodologie</a>`;
}
function underRepresented(code) {
  const ms = code === 'ALL' ? [] : [macroOf(code)];
  if (ms.includes('E')) return 'Les sciences du numérique publient beaucoup en conférence : les communications de colloque n’étant pas comptées, cette discipline est sous-représentée.';
  if (ms.includes('D')) return 'En SHS, une grande part de la production paraît en ouvrages et chapitres, non comptés ici : cette discipline est sous-représentée.';
  return '';
}
function calcBox(def, rows, result, extra = '') {
  return `<details class="app-calc"><summary class="app-calc__title">Détail du calcul</summary><p class="app-calc__def">${def}</p>
    <table class="app-calc__tab">${rows.map(r => `<tr><th scope="row">${r[0]}</th><td>${r[1]}</td></tr>`).join('')}</table>
    <p class="app-calc__res">${result}</p>${extra}</details>`;
}

/* ======================================================================
   Filtres et état
   ====================================================================== */
function discOptions(sel, withAgg = true) {
  sel.innerHTML = '';
  if (withAgg) sel.add(new Option('Toutes disciplines (38 axes)', 'ALL'));
  T.macros.forEach(m => {
    const g = document.createElement('optgroup'); g.label = `${m.code} — ${m.label}`;
    if (withAgg) g.appendChild(new Option(`${m.code} · Ensemble (${(T.axesOf[m.code] || []).length} axes)`, m.code));
    (T.axesOf[m.code] || []).forEach(c => g.appendChild(new Option(`${c} · ${T.axis[c].label}`, c)));
    sel.appendChild(g);
  });
  if (T.taxes.length) {
    const g = document.createElement('optgroup'); g.label = `${T.hm.code} — ${T.hm.label} (recoupent les axes A à G, non additionnés)`;
    T.taxes.forEach(a => g.appendChild(new Option(`${a.code} · ${a.label}`, a.code)));
    sel.appendChild(g);
  }
}
function buildFilters() {
  discOptions($('#f-disc'));
  const fr = $('#f-region'); fr.innerHTML = '';
  fr.add(new Option('France entière', ''));
  D.entities.filter(e => e.type === 'region').sort((a, b) => a.label.localeCompare(b.label, 'fr')).forEach(e => fr.add(new Option(e.label, e.label)));
  if (D.entities.some(e => e.type === 'etab' && !E['R:' + e.region])) fr.add(new Option('Organismes nationaux', '__NAT__'));
  ['#f-y0', '#f-y1'].forEach(s => { const f = $(s); f.innerHTML = ''; Y.forEach(y => f.add(new Option(y, y))); });
}
function fillEtabs() {
  const fe = $('#f-etab'); fe.innerHTML = '';
  fe.add(new Option(state.region && state.region !== '__NAT__' ? 'Toute la région' : 'Tous', ''));
  const list = D.entities.filter(e => e.type === 'etab' && (!state.region || (state.region === '__NAT__' ? !E['R:' + e.region] : e.region === state.region)));
  const groups = {};
  list.forEach(e => { const k = E['R:' + e.region] ? e.region : 'Organismes nationaux'; (groups[k] = groups[k] || []).push(e); });
  Object.keys(groups).sort((a, b) => a.localeCompare(b, 'fr')).forEach(k => {
    const g = document.createElement('optgroup'); g.label = k;
    groups[k].sort((a, b) => a.label.localeCompare(b.label, 'fr')).forEach(e => g.appendChild(new Option(e.label, e.id)));
    fe.appendChild(g);
  });
  if (state.etab && !list.some(e => e.id === state.etab)) state.etab = '';
  fe.value = state.etab;
}
function syncControls() {
  $('#f-disc').value = state.disc; $('#f-region').value = state.region; fillEtabs();
  $('#f-y0').value = state.y0; $('#f-y1').value = state.y1;
  [['seg-intl', state.intl], ['seg-bar', state.bar], ['seg-mx', state.mx], ['seg-mxset', state.mxset]].forEach(([n, v]) => { const i = $(`input[name="${n}"][value="${v}"]`); if (i) i.checked = true; });
}
function bindFilters() {
  $('#f-disc').addEventListener('change', e => { state.disc = e.target.value; update(); });
  $('#f-region').addEventListener('change', e => { state.region = e.target.value; state.etab = ''; syncControls(); update(); });
  $('#f-etab').addEventListener('change', e => { state.etab = e.target.value; syncControls(); update(); });
  $('#f-y0').addEventListener('change', e => { state.y0 = +e.target.value; if (state.y0 > state.y1) state.y1 = state.y0; syncControls(); update(); });
  $('#f-y1').addEventListener('change', e => { state.y1 = +e.target.value; if (state.y1 < state.y0) state.y0 = state.y1; syncControls(); update(); });
  $('#f-reset').addEventListener('click', () => { resetState(); syncControls(); update(); });
  [['seg-intl', 'intl'], ['seg-bar', 'bar'], ['seg-mx', 'mx'], ['seg-mxset', 'mxset']].forEach(([n, k]) =>
    $$(`input[name="${n}"]`).forEach(i => i.addEventListener('change', () => { state[k] = i.value; update(); })));
  $('#btn-csv').addEventListener('click', exportCsv);
  $('#btn-intl-csv').addEventListener('click', exportIntlCsv);
  window.addEventListener('hashchange', () => { if (readHash()) { syncControls(); render(); } });
  let t; window.addEventListener('resize', () => { clearTimeout(t); t = setTimeout(render, 150); });
}
function resetState() { Object.assign(state, { disc: 'ALL', region: '', etab: '', y0: Y[0], y1: Y[Y.length - 1] }); }
function writeHash() {
  const p = new URLSearchParams();
  if (state.disc !== 'ALL') p.set('d', state.disc);
  if (state.region) p.set('r', state.region);
  if (state.etab) p.set('e', state.etab);
  if (state.y0 !== Y[0]) p.set('y0', state.y0);
  if (state.y1 !== Y[Y.length - 1]) p.set('y1', state.y1);
  if (state.intl !== 't10') p.set('i', state.intl);
  if (state.bar !== 't10') p.set('b', state.bar);
  if (state.mx !== 't10') p.set('m', state.mx);
  if (state.mxset !== 'd') p.set('mt', state.mxset);
  const h = p.toString();
  if (location.hash.slice(1) !== h) history.replaceState(null, '', h ? '#' + h : location.pathname + location.search);
}
function readHash() {
  const p = new URLSearchParams(location.hash.slice(1)), before = JSON.stringify(state);
  resetState();
  const d = p.get('d'); if (d && (isMacro(d) || isAxis(d))) state.disc = d;
  const r = p.get('r'); if (r && (E['R:' + r] || r === '__NAT__')) state.region = r;
  const e = p.get('e'); if (e && E[e] && E[e].type === 'etab') state.etab = e;
  const y0 = +p.get('y0'), y1 = +p.get('y1');
  if (Y.includes(y0)) state.y0 = y0; if (Y.includes(y1)) state.y1 = y1;
  if (state.y0 > state.y1) [state.y0, state.y1] = [state.y1, state.y0];
  state.mxset = p.get('mt') === 't' ? 't' : 'd';
  ['i:intl', 'b:bar', 'm:mx'].forEach(x => { const [h, k] = x.split(':'); if (IND[p.get(h)]) state[k] = p.get(h); });
  return JSON.stringify(state) !== before;
}
function update() { writeHash(); render(); }

/* ======================================================================
   Infobulle
   ====================================================================== */
const tip = () => $('#tooltip');
function showTip(evt, html) {
  const t = tip(); t.innerHTML = html; t.hidden = false;
  const r = t.getBoundingClientRect();
  let x = evt.clientX + 14, y = evt.clientY + 14;
  if (x + r.width > window.innerWidth - 8) x = evt.clientX - r.width - 14;
  if (y + r.height > window.innerHeight - 8) y = evt.clientY - r.height - 14;
  t.style.left = Math.max(8, x) + 'px'; t.style.top = Math.max(8, y) + 'px';
}
function hideTip() { tip().hidden = true; }
function discTip(eid, code, W) {
  const rows = ['t10', 'tj'].map(ind => { const m = stat(eid, code, ind, W);
    return `<tr><td>${IND[ind].label}</td><td><strong>${fPct2(m.s)}</strong> <span class="hint">(${fInt(m.n)} / ${fInt(m.w)})</span></td></tr>
      <tr><td class="hint">évolution P1 → P2</td><td class="hint">${fPct2(m.s1)} → ${fPct2(m.s2)} (${fEvo(m.evo)})${m.rk ? ` · rang ${m.rk.rank}` : ''}</td></tr>`; }).join('');
  return `<strong>${esc(discName(code))}</strong><br><span class="hint">${esc(E[eid].label)}</span><table>${rows}</table>`
    + (code !== state.disc ? '<div class="hint">Cliquer pour sélectionner cette discipline.</div>' : '');
}
function attachHit(g, eid, code, W) {
  g.setAttribute('class', (g.getAttribute('class') || '') + ' hit'); g.setAttribute('tabindex', '0'); g.setAttribute('role', 'button');
  g.setAttribute('aria-label', discName(code));
  g.addEventListener('mousemove', e => showTip(e, discTip(eid, code, W)));
  g.addEventListener('mouseleave', hideTip);
  const go = () => { hideTip(); if (code !== state.disc) { state.disc = code; syncControls(); update(); } };
  g.addEventListener('click', go);
  g.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); go(); } });
}

/* ======================================================================
   Rendu
   ====================================================================== */
function render() {
  if (!D) return;
  hideTip();
  const eid = entityId(), eLabel = E[eid] ? E[eid].label : '—', W = windows();
  $('#perimeter').innerHTML = `<strong>${esc(eLabel)}</strong> · ${esc(state.disc === 'ALL' ? 'Toutes disciplines' : discName(state.disc))} · ${yrs(W.all)}`;
  $('#scope-note').innerHTML = scopeText();
  renderKpis(eid, eLabel, W);
  $('#intl').hidden = !isNational() || !PANEL.all.length;
  if (isNational()) renderIntl(W);
  renderBars(eid, eLabel, W);
  renderMatrix(eid, eLabel, W);
  renderTrends(eid, eLabel, W);
  renderTable(eid, eLabel, W);
  renderJournals();
  $('#method-generated').textContent = `Données générées le ${D.meta.generated} — ${D.meta.source}.`;
}

/* ---------------------------------------------------------------- Chiffres clés */
function renderKpis(eid, eLabel, W) {
  const nat = eid === 'FR';
  const line = (a, b) => `<p class="app-kpi__line"><span>${a}</span><strong>${b}</strong></p>`;
  const tile = ind => {
    const m = stat(eid, state.disc, ind, W), I = IND[ind];
    return `<div class="fr-col-12 fr-col-md-6"><div class="app-kpi">
      <p class="app-kpi__kicker">${I.theme} <span>— ${I.themeDef}</span></p>
      <p class="app-kpi__label">${I.label} (${yrs(m.ys)})</p>
      <p class="app-kpi__value">${fPct2(m.s)}</p>
      <p class="app-kpi__unit"><strong>${I.whatDisc(state.disc)}</strong> ont ${affilText(eid)}.</p>
      ${I.period(CE, state.y1) ? `<p class="app-kpi__period">${I.period(CE, state.y1)}</p>` : ''}
      ${line('Nombre', `${fInt(m.n)} articles sur ${fInt(m.w)} dans le monde`)}
      ${nat ? line('Rang parmi les pays', `${rk(m.rk)} · ${rk(m.rkEU)} en Europe`) : line('Part de la France', `${fPct(m.sFR)} des ${fInt(m.nFR)} articles français`)}
      ${line(`Évolution P1 (${yrs(m.p1)}) → P2 (${yrs(m.p2)})`, `${fPct2(m.s1)} → ${fPct2(m.s2)} <span class="${m.evo >= 0 ? 'app-pos' : 'app-neg'}">${fEvo(m.evo)}</span>`)}
      ${nat && m.rk1 && m.rk2 ? line('Rang P1 → P2', `${ordinal(m.rk1)} → ${ordinal(m.rk2)}`) : ''}
      ${calcBox(`${I.def}${state.disc === 'ALL' || isMacro(state.disc) ? `, en additionnant les ${members(state.disc).length} axes de la sélection` : ''}, sur ${yrs(m.ys)}.`,
        [[esc(eLabel), fInt(m.n)], ['Monde', fInt(m.w)]], `${fInt(m.n)} ÷ ${fInt(m.w)} = <strong>${fPct2(m.s)}</strong>`,
        `<p class="app-calc__read">Évolution : part sur P2 ÷ part sur P1 − 1 = ${fPct2(m.s2)} ÷ ${fPct2(m.s1)} − 1 = <strong>${fEvo(m.evo)}</strong>.</p>`
        + (ind === 'tj' ? `<p class="app-calc__read">Chaque axe a ses propres 10 revues. <a class="fr-link fr-link--sm" href="#revues" data-journals="${esc(isAxis(state.disc) ? state.disc : members(state.disc)[0])}">Voir les listes</a></p>` : ''))}
      ${m.low ? '<p class="app-kpi__note">Effectif faible : valeur peu fiable.</p>' : ''}
    </div></div>`;
  };
  $('#kpis').innerHTML = tile('t10') + tile('tj')
    + (isTrans(state.disc) ? `<div class="fr-col-12"><div class="fr-alert fr-alert--info fr-alert--sm"><p>Axe transverse : il recoupe des axes disciplinaires (ses articles y sont aussi comptés) et n’entre pas dans « Toutes disciplines » ni dans les macro-disciplines. Ses revues de tête sont ses 10 revues SJR propres.</p></div></div>` : '')
    + (underRepresented(state.disc) ? `<div class="fr-col-12"><div class="fr-alert fr-alert--info fr-alert--sm"><p>${underRepresented(state.disc)}</p></div></div>` : '');
}

/* ---------------------------------------------------------------- Comparaison internationale */
function renderIntl(W) {
  const I = IND[state.intl], { ys } = yearsOf(state.intl, W);
  const ids = PANEL.all.filter(id => D.cube[id]);
  const val = (id, y) => share(id, state.disc, I.k, [y]);
  const rankAt = (set, y) => rankAmong(set.filter(id => ids.includes(id)), 'FR', id => val(id, y));
  const y0 = ys[0], y1 = ys[ys.length - 1], eu = ['FR', ...PANEL.eu];
  const a = ys.length && rankAt(ids, y0), b = ys.length && rankAt(ids, y1), ea = ys.length && rankAt(eu, y0), eb = ys.length && rankAt(eu, y1);
  const arrow = (x, y) => !x || !y ? '' : y.rank < x.rank ? ' <span class="app-pos">▲</span>' : y.rank > x.rank ? ' <span class="app-neg">▼</span>' : '';
  $('#intl-sub').innerHTML = `<strong>${I.label}</strong>, par pays et par année : ${I.def}. Équivalent : ${I.ost}.<br>`
    + (ys.length ? `<strong>France : ${ordinal(a)} en ${y0} → ${ordinal(b)} en ${y1} sur ${b ? b.of : '–'} pays</strong>${arrow(a, b)} · parmi les pays européens : ${ordinal(ea)} → ${ordinal(eb)} sur ${eb ? eb.of : '–'}${arrow(ea, eb)}.` : '');
  drawRibbons({ label: I.label, fmt: fPct2, val: (id, yy) => share(id, state.disc, I.k, yy) }, ys, ids);
}

/* ---------------------------------------------------------------- Les 38 disciplines */
function renderBars(eid, eLabel, W) {
  const host = $('#chart-bars'); host.innerHTML = '';
  const ind = state.bar, I = IND[ind], all = stat(eid, 'ALL', ind, W);
  const mk = list => { const r = list.map(a => ({ code: a.code, m: stat(eid, a.code, ind, W) })); if (state.sortBars) r.sort((a, b) => (b.m.s || 0) - (a.m.s || 0)); return r; };
  const rowsD = mk(T.axes), rowsT = mk(T.taxes), hdr = rowsT.length ? 1 : 0;
  const rows = [...rowsD, ...(hdr ? [{ hdr: true }] : []), ...rowsT];
  const Wd = Math.max(320, host.clientWidth - 16), narrow = Wd < 700;
  const labelW = narrow ? 64 : Math.min(330, Wd * 0.34), rowH = 22, top = narrow ? 30 : 44, H = top + rows.length * rowH + 26;
  const svg = el('svg', { viewBox: `0 0 ${Wd} ${H}`, role: 'img', 'aria-label': `${I.label} par discipline` }, host);
  const x0 = labelW + 8, x1 = Wd - (narrow ? 90 : 130);
  const max = Math.max(...rows.filter(r => !r.hdr).map(r => r.m.s).filter(ok), all.s) * 1.08 || 1;
  const sx = v => x0 + Math.max(0, v) / max * (x1 - x0);
  niceTicks(0, max, narrow ? 3 : 5).forEach(t => { el('line', { x1: sx(t), x2: sx(t), y1: top - 4, y2: H - 22, class: 'grid' }, svg);
    txt(svg, sx(t), H - 8, fPct(t), { 'text-anchor': 'middle', 'font-size': 11, class: 't-muted' }); });
  if (!narrow) txt(svg, x0, 12, `${I.label} (${yrs(all.ys)}) — ${eLabel}`, { 'font-size': 12, class: 't-muted' });
  const hl = state.disc === 'ALL' ? null : new Set(members(state.disc));
  rows.forEach((r, i) => {
    const y = top + i * rowH;
    if (r.hdr) {
      el('rect', { x: 0, y: y + 2, width: Wd, height: rowH - 2, class: 'q-bg', 'fill-opacity': 0.9 }, svg);
      txt(svg, 8, y + 16, narrow ? `${T.hm.code} · transverses (non additionnés)` : `${T.hm.code} · ${T.hm.label} — recoupent les axes ci-dessus, non inclus dans « toutes disciplines »`, { 'font-size': 12, 'font-weight': 700 });
      return;
    }
    const g = el('g', {}, svg), on = !hl || hl.has(r.code);
    if ((!state.sortBars && i > 0 && !rows[i - 1].hdr && macroOf(rows[i - 1].code) !== macroOf(r.code)) || i === 0)
      el('line', { x1: 0, x2: Wd, y1: y, y2: y, class: 'grid', 'stroke-opacity': i ? 1 : 0 }, svg);
    el('rect', { x: 0, y, width: Wd, height: rowH, fill: 'transparent' }, g);
    txt(g, labelW, y + 15, narrow ? r.code : truncate(`${r.code} · ${T.axis[r.code].label}`, Math.floor(labelW / 6.3)),
      { 'text-anchor': 'end', 'font-size': 12, 'font-weight': state.disc === r.code ? 700 : 400 });
    el('rect', { x: x0, y: y + 4, width: Math.max(1, sx(r.m.s || 0) - x0), height: rowH - 8, rx: 1, fill: colorOf(r.code),
      'fill-opacity': r.m.low ? 0.3 : on ? 1 : 0.3, class: 'bar' }, g);
    txt(g, sx(r.m.s || 0) + 5, y + 15, fPct2(r.m.s), { 'font-size': 11, 'font-weight': state.disc === r.code ? 700 : 400 });
    if (!narrow) txt(g, x1 + 70, y + 15, fEvo(r.m.evo), { 'font-size': 11, 'text-anchor': 'end', class: r.m.evo >= 0 ? 't-pos' : 't-neg' });
    attachHit(g, eid, r.code, W);
  });
  if (ok(all.s)) { el('line', { x1: sx(all.s), x2: sx(all.s), y1: top - 6, y2: H - 22, class: 'refline' }, svg);
    txt(svg, sx(all.s) + 4, top - 8, `Toutes disciplines : ${fPct2(all.s)}`, { 'font-size': 11, class: 't-muted' }); }
  if (!narrow) txt(svg, x1 + 70, top - 8, 'P1 → P2', { 'font-size': 11, 'text-anchor': 'end', class: 't-muted' });
  $('#bars-sub').innerHTML = `<strong>${I.label}</strong> : ${I.def}. Une barre par axe : les 38 axes disciplinaires, colorés par macro-discipline, puis les ${T.taxes.length} axes transverses (gris), qui les recoupent ; à droite, l’évolution de la part entre P1 (${yrs(all.p1)}) et P2 (${yrs(all.p2)}). Pointillés : valeur toutes disciplines ${esc(ofWho(eLabel))}. Cliquer sur un axe pour le sélectionner.`;
  $('#bars-legend').innerHTML = T.macros.map(m => `<span class="app-legend__item"><span class="app-swatch" style="background:var(--m-${m.code})"></span>${m.code} · ${esc(m.short)}</span>`).join('')
    + (T.taxes.length ? `<span class="app-legend__item"><span class="app-swatch" style="background:var(--m-${T.hm.code})"></span>${T.hm.code} · ${esc(T.hm.short)}</span>` : '')
    + `<label class="app-legend__item" style="margin-left:auto;cursor:pointer"><input type="checkbox" id="sort-bars" ${state.sortBars ? 'checked' : ''}> Trier par valeur</label>`;
  $('#sort-bars').addEventListener('change', e => { state.sortBars = e.target.checked; render(); });
}

/* ---------------------------------------------------------------- Matrice des 38 disciplines */
function renderMatrix(eid, eLabel, W) {
  const host = $('#chart-quad'); host.innerHTML = '';
  const ind = state.mx, I = IND[ind], all = stat(eid, 'ALL', ind, W);
  const set = state.mxset === 't' && T.taxes.length ? T.taxes : T.axes;
  const pts = set.map(a => ({ code: a.code, m: stat(eid, a.code, ind, W) })).filter(p => p.m.s > 0 && ok(p.m.evo));
  $('#quad-sub').innerHTML = `Chaque bulle est un axe ${set === T.taxes ? 'transverse' : 'disciplinaire'} : à quel point ${esc(who(eLabel))} y pèse dans le monde (horizontal) et comment ce poids a évolué (vertical). Les deux axes de lecture sont expliqués sous le graphique.`;
  const Wd = Math.max(320, host.clientWidth - 16), H = Math.round(Math.min(600, Math.max(360, Wd * 0.7)));
  const m = { l: 56, r: 16, t: 16, b: 46 };
  const svg = el('svg', { viewBox: `0 0 ${Wd} ${H}`, role: 'img', 'aria-label': set === T.taxes ? 'Matrice des axes transverses' : 'Matrice des 38 disciplines' }, host);
  if (!pts.length || !ok(all.s) || !ok(all.evo)) { txt(svg, Wd / 2, H / 2, 'Période trop courte ou effectifs insuffisants.', { 'text-anchor': 'middle', class: 't-muted' }); $('#quad-key').innerHTML = ''; $('#quad-how').innerHTML = ''; return; }
  const tx = v => Math.log2(v / all.s), ty = v => v - all.evo;
  const xs = pts.map(p => tx(p.m.s)), ysv = pts.map(p => ty(p.m.evo));
  const xr = Math.max(0.5, ...xs.map(Math.abs)) * 1.12, yr = Math.max(0.05, ...ysv.map(Math.abs)) * 1.15;
  const sx = v => m.l + (v + xr) / (2 * xr) * (Wd - m.l - m.r), sy = v => H - m.b - (v + yr) / (2 * yr) * (H - m.t - m.b);
  const cx = sx(0), cy = sy(0);
  const Q = { tr: ['Points forts qui se renforcent', 'good'], br: ['Points forts qui s’érodent', 'mid'], tl: ['Positions en progression', 'mid'], bl: ['Points faibles qui reculent', 'bad'] };
  [['tl', m.l, m.t, cx - m.l, cy - m.t], ['tr', cx, m.t, Wd - m.r - cx, cy - m.t], ['bl', m.l, cy, cx - m.l, H - m.b - cy], ['br', cx, cy, Wd - m.r - cx, H - m.b - cy]].forEach(([k, x, y, w, h]) => {
    el('rect', { x, y, width: w, height: h, class: 'q-bg' + (Q[k][1] === 'good' ? ' q-bg--good' : Q[k][1] === 'bad' ? ' q-bg--bad' : ''), 'fill-opacity': 0.55 }, svg);
    const left = k.endsWith('l'), topq = k.startsWith('t');
    if (Wd >= 600) txt(svg, left ? x + 8 : x + w - 8, topq ? y + 16 : y + h - 8, Q[k][0], { 'text-anchor': left ? 'start' : 'end', class: 'quad-label' });
  });
  // graduations : parts réelles en abscisse, évolutions réelles en ordonnée
  const xticks = []; for (let e = -6; e <= 6; e++) { const v = all.s * Math.pow(2, e / 2); if (Math.abs(e / 2) <= xr) xticks.push(v); }
  xticks.forEach(v => { const X = sx(tx(v)); el('line', { x1: X, x2: X, y1: m.t, y2: H - m.b, class: 'grid', 'stroke-opacity': 0.5 }, svg);
    txt(svg, X, H - m.b + 16, fPct2(v), { 'text-anchor': 'middle', 'font-size': 10, class: 't-muted' }); });
  niceTicks(all.evo - yr, all.evo + yr, 6).forEach(v => { const Yp = sy(ty(v)); el('line', { x1: m.l, x2: Wd - m.r, y1: Yp, y2: Yp, class: 'grid', 'stroke-opacity': 0.5 }, svg);
    txt(svg, m.l - 6, Yp + 4, fEvo(v), { 'text-anchor': 'end', 'font-size': 10, class: 't-muted' }); });
  if (Math.abs(all.evo) < yr) { const Y0 = sy(ty(0)); el('line', { x1: m.l, x2: Wd - m.r, y1: Y0, y2: Y0, class: 'refline' }, svg);
    txt(svg, cx - 6, Y0 + 13, 'évolution nulle', { 'text-anchor': 'end', 'font-size': 10, class: 't-muted' }); }
  el('line', { x1: cx, x2: cx, y1: m.t, y2: H - m.b, class: 'axis0' }, svg);
  el('line', { x1: m.l, x2: Wd - m.r, y1: cy, y2: cy, class: 'axis0' }, svg);
  txt(svg, (m.l + Wd - m.r) / 2, H - 8, `${I.label} →`, { 'text-anchor': 'middle', 'font-size': 12 });
  const yl = txt(svg, 0, 0, 'Évolution P1 → P2 →', { 'text-anchor': 'middle', 'font-size': 12, transform: `translate(14 ${(m.t + H - m.b) / 2}) rotate(-90)` });
  yl.removeAttribute('x'); yl.removeAttribute('y');
  const maxN = Math.max(...pts.map(p => p.m.n)), rMax = Math.min(26, Wd / 26), labels = [];
  pts.slice().sort((a, b) => b.m.n - a.m.n).forEach(p => {
    const g = el('g', {}, svg), X = sx(tx(p.m.s)), Yp = sy(ty(p.m.evo)), r = Math.max(4, rMax * Math.sqrt(p.m.n / maxN));
    const on = state.disc === 'ALL' || members(state.disc).includes(p.code);
    el('circle', { cx: X, cy: Yp, r, fill: colorOf(p.code), 'fill-opacity': on ? 0.8 : 0.25, class: 'bubble' + (p.m.low ? ' low' : ''), stroke: p.m.low ? colorOf(p.code) : null }, g);
    if (p.code === state.disc) el('circle', { cx: X, cy: Yp, r: r + 3, fill: 'none', stroke: 'var(--text-title-grey)', 'stroke-width': 2 }, g);
    labels.push({ X, Y: Yp, r, s: p.code });
    attachHit(g, eid, p.code, W);
  });
  const placed = [], hits = b => placed.some(o => b.x0 < o.x1 && b.x1 > o.x0 && b.y0 < o.y1 && b.y1 > o.y0);
  labels.sort((a, b) => b.r - a.r).forEach(l => {
    const w = l.s.length * 6.4, h = 12;
    const c = [{ x: l.X, y: l.Y - l.r - 3, a: 'middle' }, { x: l.X, y: l.Y + l.r + 12, a: 'middle' }, { x: l.X + l.r + 3, y: l.Y + 4, a: 'start' }, { x: l.X - l.r - 3, y: l.Y + 4, a: 'end' }]
      .map(o => ({ ...o, x0: o.a === 'middle' ? o.x - w / 2 : o.a === 'start' ? o.x : o.x - w, y0: o.y - h + 2 })).map(o => ({ ...o, x1: o.x0 + w, y1: o.y0 + h }));
    const best = c.find(o => !hits(o) && o.x0 >= m.l && o.x1 <= Wd - m.r && o.y0 >= m.t && o.y1 <= H - m.b);
    if (!best) return;           // pas de place : le code reste lisible au survol
    placed.push(best); txt(svg, best.x, best.y, l.s, { 'text-anchor': best.a, class: 'bubble-label', 'font-size': 11 });
  });
  const groups = { tl: [], tr: [], bl: [], br: [] };
  pts.forEach(p => groups[(ty(p.m.evo) >= 0 ? 't' : 'b') + (tx(p.m.s) >= 0 ? 'r' : 'l')].push(p));
  const big = k => groups[k].slice().sort((a, b) => b.m.n - a.m.n)[0];
  const ex = ['tr', 'br', 'tl', 'bl'].filter(k => big(k)).map(k => [k, big(k)])[0];
  const exTxt = ex ? `<p class="fr-text--sm fr-mb-0"><strong>Exemple</strong> : ${esc(ex[1].code)} ${esc(T.axis[ex[1].code].label)} a une part de ${fPct2(ex[1].m.s)} (${ex[1].m.s >= all.s ? 'au-dessus' : 'en dessous'} de ${fPct2(all.s)}) et une évolution de ${fEvo(ex[1].m.evo)} (${ex[1].m.evo >= all.evo ? 'meilleure' : 'moins bonne'} que ${fEvo(all.evo)}) : ${Q[ex[0]][0].toLowerCase()}.</p>` : '';
  $('#quad-how').innerHTML = `<h3 class="fr-text--md fr-text--bold fr-mb-1w">Comment les axes de la matrice sont déterminés</h3><ul class="fr-text--sm">
    <li><strong>Position horizontale d’une bulle</strong> : ${I.label.toLowerCase()} ${esc(ofWho(eLabel))} dans l’axe, sur ${yrs(all.ys)}. L’échelle est logarithmique (chaque graduation multiplie la part par 1,41) pour que les axes à faible part restent lisibles.</li>
    <li><strong>Position verticale</strong> : évolution de cette part entre P1 (${yrs(all.p1)}) et P2 (${yrs(all.p2)}), soit part P2 ÷ part P1 − 1.</li>
    <li><strong>Ligne verticale de séparation</strong> : la part ${esc(ofWho(eLabel))} <strong>toutes disciplines</strong>, soit <strong>${fPct2(all.s)}</strong> (somme des 38 axes disciplinaires, pas moyenne des axes). À droite : les disciplines où la part mondiale ${esc(ofWho(eLabel))} dépasse ce niveau (points forts relatifs) ; à gauche, celles où elle est inférieure.</li>
    <li><strong>Ligne horizontale de séparation</strong> : l’évolution toutes disciplines, soit <strong>${fEvo(all.evo)}</strong>. Au-dessus : les disciplines qui évoluent mieux que l’ensemble, même si leur part baisse ; en dessous, moins bien. La ligne pointillée marque l’évolution nulle (0 %).</li>
    <li><strong>Pourquoi cette référence ?</strong> La valeur toutes disciplines est la position globale ${esc(ofWho(eLabel))} : la prendre comme centre fait ressortir ses forces et faiblesses <em>relatives</em>, et neutralise la baisse commune à presque tous les pays (montée de la Chine, meilleure couverture des affiliations dans OpenAlex), qui sinon placerait toutes les disciplines en bas.</li>
    <li><strong>Taille des bulles</strong> : nombre d’articles ${esc(ofWho(eLabel))} comptés dans l’indicateur.${set === T.taxes ? ` Les axes transverses sont placés par rapport à la même croix que les axes disciplinaires.` : ''}</li>
  </ul>${exTxt}`;
  $('#quad-key').innerHTML = `<div class="app-quad-key">${['tr', 'br', 'tl', 'bl'].map(k => `<div class="app-quad-key__cell app-quad-key__cell--${Q[k][1]}">
    <h3>${Q[k][0]} <span class="fr-text--xs fr-text-mention--grey">(${groups[k].length})</span></h3>${groups[k].length ? `<ul>${groups[k].sort((a, b) => b.m.n - a.m.n).map(p => `<li>${esc(p.code)} ${esc(truncate(T.axis[p.code].label, 34))}</li>`).join('')}</ul>` : '<p><em>Aucune</em></p>'}</div>`).join('')}</div>`;
}

/* ---------------------------------------------------------------- Évolution annuelle */
function renderTrends(eid, eLabel, W) {
  $('#trend-sub').innerHTML = `${esc(eLabel)} — ${esc(state.disc === 'ALL' ? 'toutes disciplines' : discName(state.disc))}, part de chaque année.`;
  ['t10', 'tj'].forEach(ind => {
    const I = IND[ind], { ys } = yearsOf(ind, W);
    lineChart($(`#chart-t-${ind}`), `${I.label} (${yrs(ys)})`, ys, [{ v: ys.map(y => share(eid, state.disc, I.k, [y])), color: 'var(--bar-fr)', name: eLabel }], fPct2, true);
  });
}

/* ---------------------------------------------------------------- Tableau détaillé */
let lastRows = [];
function renderTable(eid, eLabel, W) {
  const nat = eid === 'FR', t = $('#detail-table');
  const codes = ['ALL']; T.macros.forEach(mc => { codes.push(mc.code); (T.axesOf[mc.code] || []).forEach(a => codes.push(a)); });
  if (T.taxes.length) { codes.push('__H'); T.taxes.forEach(a => codes.push(a.code)); }
  lastRows = codes.map(c => c === '__H' ? { code: c, hdr: true } : ({ code: c, a: stat(eid, c, 't10', W), b: stat(eid, c, 'tj', W) }));
  const grp = (I, w) => `<th scope="colgroup" colspan="${nat ? 5 : 4}" class="app-th-group">${I.label} (${yrs(w)})</th>`;
  const sub = `<th scope="col" class="num">Nombre</th><th scope="col" class="num">Monde</th><th scope="col" class="num">Part</th><th scope="col" class="num" title="Part sur P2 ÷ part sur P1 − 1">Évol. P1→P2</th>${nat ? '<th scope="col" class="num" title="Rang de la France parmi les pays des panels, selon le nombre (donc la part)">Rang</th>' : ''}`;
  t.querySelector('thead').innerHTML = `<tr><th scope="col" rowspan="2">Discipline</th>${grp(IND.t10, W.cit)}${grp(IND.tj, W.all)}</tr><tr>${sub}${sub}</tr>`;
  const cells = m => `<td class="num">${fInt(m.n)}</td><td class="num app-muted">${fInt(m.w)}</td><td class="num"><strong>${fPct2(m.s)}</strong></td>`
    + `<td class="num ${ok(m.evo) ? (m.evo >= 0.02 ? 'app-pos' : m.evo <= -0.02 ? 'app-neg' : '') : ''}" title="${fPct2(m.s1)} → ${fPct2(m.s2)}">${fEvo(m.evo)}</td>${nat ? `<td class="num">${m.rk ? m.rk.rank : '–'}</td>` : ''}`;
  t.querySelector('tbody').innerHTML = lastRows.map(r => {
    if (r.hdr) return `<tr class="is-parent is-hdr"><td class="disc" colspan="${nat ? 11 : 9}">${T.hm.code} · ${esc(T.hm.label)} <span class="fr-text--xs fr-text-mention--grey">— recoupent les axes A à G : pas de sous-total, non inclus dans « toutes disciplines »</span></td></tr>`;
    const cls = r.code === 'ALL' ? 'is-total' : isMacro(r.code) ? 'is-parent' : '';
    const sel = r.code === state.disc ? ' is-selected' : '', lowc = r.a.low ? ' <span class="fr-text--xs fr-text-mention--grey">(effectif faible)</span>' : '';
    const name = r.code === 'ALL' ? 'Toutes disciplines (38 axes)' : isMacro(r.code) ? `${r.code} · ${T.macro[r.code].label}` : `<span class="disc-code">${r.code}</span>${esc(T.axis[r.code].label)}`;
    return `<tr class="${cls}${sel}" data-code="${r.code}"><td class="disc">${name}${lowc}</td>${cells(r.a)}${cells(r.b)}</tr>`;
  }).join('');
  $$('#detail-table tbody tr:not(.is-hdr)').forEach(tr => tr.addEventListener('click', () => { state.disc = tr.dataset.code; syncControls(); update(); }));
  $('#table-sub').innerHTML = `${esc(eLabel)}. Une macro-discipline et « toutes disciplines » additionnent les comptes de leurs axes disciplinaires ; les axes transverses sont donnés à part. Évolution : part sur P2 ÷ part sur P1 − 1 (top 10 % : ${yrs(W.c1)} → ${yrs(W.c2)} ; revues : ${yrs(W.a1)} → ${yrs(W.a2)}).${nat ? ` Rang : parmi les ${PANEL.all.length} pays des panels.` : ''} Cliquer sur une ligne pour la sélectionner.`;
}
function csvNum(x, d = 5) { return ok(x) ? String(+x.toFixed(d)).replace('.', ',') : ''; }
function exportCsv() {
  const eid = entityId(), W = windows(), nat = eid === 'FR';
  const h = ind => { const I = IND[ind].short; return [`${I} — nombre`, `${I} — monde`, `${I} — part`, `${I} — part P1`, `${I} — part P2`, `${I} — évolution`, ...(nat ? [`${I} — rang`] : [])]; };
  const v = m => [m.n, m.w, csvNum(m.s), csvNum(m.s1), csvNum(m.s2), csvNum(m.evo, 4), ...(nat ? [m.rk ? rkTxt(m.rk) : ''] : [])];
  const head = ['Entité', 'Niveau', 'Code', 'Discipline', ...h('t10'), ...h('tj')];
  downloadCsv([head, ...lastRows.filter(r => !r.hdr).map(r => [E[eid].label, r.code === 'ALL' ? 'total' : isMacro(r.code) ? 'macro' : isTrans(r.code) ? 'axe transverse' : 'axe', r.code, discLabel(r.code), ...v(r.a), ...v(r.b)])],
    `bibliometrie_${E[eid].label.replace(/[^\wÀ-ſ-]+/g, '_')}_${yrs(W.all)}.csv`);
}
function exportIntlCsv() {
  const W = windows(), I = IND[state.intl], { ys } = yearsOf(state.intl, W);
  const head = ['Pays', 'Panel Europe', 'Panel monde', 'Discipline', 'Indicateur', ...ys.map(String), `Période ${yrs(ys)}`];
  downloadCsv([head, ...PANEL.all.map(id => [E[id].label, PANEL.eu.includes(id) ? 'oui' : '', PANEL.world.includes(id) ? 'oui' : '', discLabel(state.disc), I.label,
    ...ys.map(y => csvNum(share(id, state.disc, I.k, [y]))), csvNum(share(id, state.disc, I.k, ys))])], `comparaison_internationale_${state.intl}_${state.disc}.csv`);
}

const RIB_COLORS = ['#E1000F', '#C3992A', '#00A95F', '#CE614A', '#A558A0', '#009099', '#417DC4', '#7D7368', '#D9A400', '#5E2A2B',
  '#6E445A', '#3F7A5C', '#B34000', '#8B6D9C', '#4D7FA3', '#A0814D', '#2D7A8C', '#C08C65', '#68588C', '#7A9E48', '#9C4F6E', '#557A95', '#B5683E', '#4B8B6E', '#8C7A3F'];
function drawRibbons(def, ys, ids) {
  const host = $('#chart-intl'); host.innerHTML = '';
  if (!ys.length) { host.innerHTML = '<div class="app-empty">Période hors de la fenêtre de citation.</div>'; return; }
  const val = Object.fromEntries(ids.map(id => [id, ys.map(y => def.val(id, [y]))]));
  const n = ids.length, Wd = Math.max(320, host.clientWidth - 16), narrow = Wd < 640;
  const m = { l: narrow ? 70 : 150, r: narrow ? 70 : 150, t: 26, b: 28 }, gap = narrow ? 2 : 3;
  const colH = Math.max(narrow ? 360 : 420, n * (narrow ? 14 : 19), n * ((narrow ? 10 : 11) + 2) + 40);
  const maxSum = Math.max(...ys.map((_, i) => ids.reduce((t, id) => t + (ok(val[id][i]) ? val[id][i] : 0), 0)));
  const slot = (narrow ? 10 : 11) + 2;
  const k = (colH - gap * (n - 1)) / (maxSum || 1), minT = 1.5;
  // position de chaque pays chaque année
  const pos = ys.map((_, i) => {
    const order = ids.filter(id => ok(val[id][i])).sort((a, b) => val[b][i] - val[a][i]);
    let y = m.t; const o = {};
    // chaque pays occupe au moins la hauteur d'une étiquette : les étiquettes restent en face de leur ruban
    order.forEach((id, r) => { const t = Math.max(minT, val[id][i] * k); o[id] = { y0: y, y1: y + t, rank: r + 1 }; y += Math.max(t + gap, slot); });
    return o;
  });
  const H = Math.max(...pos.map(o => Math.max(m.t, ...Object.values(o).map(p => p.y1)))) + m.b;
  const svg = el('svg', { viewBox: `0 0 ${Wd} ${H}`, role: 'img', 'aria-label': `${def.label} : classement des pays par année` }, host);
  const xc = i => m.l + (ys.length === 1 ? 0.5 : i / (ys.length - 1)) * (Wd - m.l - m.r);
  const half = ys.length > 1 ? (Wd - m.l - m.r) / (ys.length - 1) * 0.18 : 20;
  ys.forEach((y, i) => txt(svg, xc(i), H - 8, y, { 'text-anchor': 'middle', 'font-size': 11, class: 't-muted' }));
  const color = {}; ids.filter(id => id !== 'FR').sort((a, b) => E[a].label.localeCompare(E[b].label, 'fr')).forEach((id, i) => (color[id] = RIB_COLORS[i % RIB_COLORS.length]));
  color.FR = 'var(--bar-fr)';
  const order = ids.slice().sort((a, b) => (a === 'FR') - (b === 'FR'));   // France dessinée en dernier
  const labsL = [], labsR = [];
  order.forEach(id => {
    const g = el('g', { class: 'rib' + (id === 'FR' ? ' rib--fr' : '') }, svg);
    // segments continus d'années où la valeur existe
    let seg = [];
    const flush = () => {
      if (!seg.length) return;
      let top = '', bot = '';
      seg.forEach((i, j) => {
        const p = pos[i][id], xa = xc(i) - half, xb = xc(i) + half;
        if (j === 0) top += `M${xa} ${p.y0}`; else {
          const q = pos[seg[j - 1]][id], xp = xc(seg[j - 1]) + half, mx = (xp + xa) / 2;
          top += ` C${mx} ${q.y0} ${mx} ${p.y0} ${xa} ${p.y0}`;
        }
        top += ` L${xb} ${p.y0}`;
      });
      for (let j = seg.length - 1; j >= 0; j--) {
        const i = seg[j], p = pos[i][id], xa = xc(i) - half, xb = xc(i) + half;
        bot += ` L${xb} ${p.y1} L${xa} ${p.y1}`;
        if (j > 0) { const q = pos[seg[j - 1]][id], xp = xc(seg[j - 1]) + half, mx = (xp + xa) / 2; bot += ` C${mx} ${p.y1} ${mx} ${q.y1} ${xp} ${q.y1}`; }
      }
      el('path', { d: top + bot + ' Z', fill: color[id], class: 'rib-band' }, g);
      seg = [];
    };
    ys.forEach((_, i) => { if (pos[i][id]) seg.push(i); else flush(); }); flush();
    const i0 = ys.findIndex((_, i) => pos[i][id]); let i1 = -1; ys.forEach((_, i) => { if (pos[i][id]) i1 = i; });
    const lab = l => truncate(l, narrow ? 9 : 18);
    if (i0 >= 0) { const p = pos[i0][id]; labsL.push({ g, id, y: (p.y0 + p.y1) / 2, x: xc(i0) - half - 6, s: narrow ? lab(E[id].label) : `${lab(E[id].label)} ${def.fmt(val[id][i0])}` }); }
    if (i1 >= 0) { const p = pos[i1][id]; labsR.push({ g, id, y: (p.y0 + p.y1) / 2, x: xc(i1) + half + 6, s: narrow ? lab(E[id].label) : `${def.fmt(val[id][i1])} ${lab(E[id].label)}` }); }
    g.addEventListener('mousemove', ev => {
      const box = svg.getBoundingClientRect(), px = (ev.clientX - box.left) * (Wd / box.width);
      const i = Math.max(0, Math.min(ys.length - 1, Math.round((px - m.l) / ((Wd - m.l - m.r) / Math.max(1, ys.length - 1)))));
      const p = pos[i][id];
      showTip(ev, `<strong>${esc(E[id].label)}</strong><table><tr><td>${ys[i]}</td><td>${def.fmt(val[id][i])}${p ? ` · ${p.rank}<sup>e</sup> sur ${Object.keys(pos[i]).length}` : ''}</td></tr>
        <tr><td>${ys[i0]} → ${ys[i1]}</td><td>${def.fmt(val[id][i0])} → ${def.fmt(val[id][i1])}</td></tr></table>`);
      g.classList.add('is-hover');
    });
    g.addEventListener('mouseleave', () => { hideTip(); g.classList.remove('is-hover'); });
  });
  // étiquettes : espacement minimal (ordre conservé), avec un trait de rappel si l'étiquette est décalée
  const fs = narrow ? 10 : 11, minGap = fs + 2;
  [[labsL, 'end', -1], [labsR, 'start', 1]].forEach(([L, anchor, dir]) => {
    L.sort((a, b) => a.y - b.y);
    L.forEach((l, i) => { l.ly = i ? Math.max(l.y, L[i - 1].ly + minGap) : l.y; });
    L.forEach(l => {
      if (Math.abs(l.ly - l.y) > 2) el('path', { d: `M${l.x - dir * 4} ${l.y} L${l.x} ${l.ly}`, stroke: color[l.id], 'stroke-width': 1, fill: 'none' }, l.g);
      txt(l.g, l.x + dir * 2, l.ly + 4, l.s, { 'text-anchor': anchor, 'font-size': fs, 'font-weight': l.id === 'FR' ? 700 : 400, fill: color[l.id] });
    });
  });
  $('#intl-legend').innerHTML = `<span class="app-legend__item"><span class="app-swatch app-swatch--fr"></span>France</span><span class="app-legend__item">Chaque couleur = un pays ; survoler un ruban pour le détail. Épaisseur proportionnelle à la valeur ; une épaisseur minimale est appliquée aux plus petites valeurs pour qu’elles restent visibles.</span>`;
}

function lineChart(host, title, years, series, fmt, zeroBase) {
  host.innerHTML = '';
  const Wd = Math.max(260, host.clientWidth - 16), H = 210, m = { l: 56, r: 12, t: 26, b: 26 };
  const svg = el('svg', { viewBox: `0 0 ${Wd} ${H}`, role: 'img', 'aria-label': title }, host);
  txt(svg, 0, 14, title, { 'font-size': 13, class: 't-title' });
  const all = series.flatMap(s => s.v).filter(ok);
  if (!all.length || !years.length) { txt(svg, Wd / 2, H / 2, 'Pas de données', { 'text-anchor': 'middle', class: 't-muted' }); return; }
  let lo = zeroBase ? 0 : Math.min(...all), hi = Math.max(...all);
  if (!zeroBase) { const pad = (hi - lo) * 0.15 || hi * 0.1 || 1; lo = Math.max(0, lo - pad); hi += pad; } else hi *= 1.15;
  const sx = i => m.l + (years.length === 1 ? 0.5 : i / (years.length - 1)) * (Wd - m.l - m.r);
  const sy = v => H - m.b - (v - lo) / (hi - lo || 1) * (H - m.t - m.b);
  niceTicks(lo, hi, 4).forEach(t => { el('line', { x1: m.l, x2: Wd - m.r, y1: sy(t), y2: sy(t), class: 'grid' }, svg);
    txt(svg, m.l - 6, sy(t) + 4, fmt(t), { 'text-anchor': 'end', 'font-size': 10, class: 't-muted' }); });
  const step = Math.ceil(years.length / Math.max(2, Math.floor((Wd - m.l) / 46)));
  years.forEach((y, i) => { if (i % step === 0 || i === years.length - 1) txt(svg, sx(i), H - 8, y, { 'text-anchor': 'middle', 'font-size': 10, class: 't-muted' }); });
  series.forEach(s => {
    let d = '', pen = false;
    s.v.forEach((v, i) => { if (ok(v)) { d += (pen ? 'L' : 'M') + sx(i).toFixed(1) + ' ' + sy(v).toFixed(1); pen = true; } else pen = false; });
    el('path', { d, fill: 'none', stroke: s.color, 'stroke-width': s.thin ? 1.5 : s.dash ? 2 : 2.5, 'stroke-opacity': s.thin ? 0.8 : 1, class: s.dash ? 'line-ref' : null }, svg);
    if (!s.dash && !s.thin) s.v.forEach((v, i) => ok(v) && el('circle', { cx: sx(i), cy: sy(v), r: 2.5, fill: s.color }, svg));
  });
  const ov = el('rect', { x: m.l, y: m.t, width: Wd - m.l - m.r, height: H - m.t - m.b, fill: 'transparent' }, svg);
  ov.addEventListener('mousemove', e => {
    const box = svg.getBoundingClientRect(), px = (e.clientX - box.left) * (Wd / box.width);
    const i = Math.max(0, Math.min(years.length - 1, Math.round((px - m.l) / ((Wd - m.l - m.r) / Math.max(1, years.length - 1)))));
    showTip(e, `<strong>${years[i]}</strong><table>${series.map(s => `<tr><td>${esc(s.name || '')}</td><td>${fmt(s.v[i])}</td></tr>`).join('')}</table>`);
  });
  ov.addEventListener('mouseleave', hideTip);
}

function downloadCsv(table, name) {
  const csv = '﻿' + table.map(r => r.map(v => `"${String(v).replace(/"/g, '""')}"`).join(';')).join('\r\n');
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' })); a.download = name;
  document.body.appendChild(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

/* ---------------------------------------------------------------- Revues */
let journalsDisc = null, journalsFollow = null;
const OA_URL = id => `https://openalex.org/${encodeURIComponent(id)}`;
function buildJournalsSelect() {
  const sel = $('#j-disc'); discOptions(sel, false);
  sel.addEventListener('change', e => { journalsDisc = e.target.value; renderJournals(); });
  $('#btn-journals-csv').addEventListener('click', () => exportJournals([journalsDisc]));
  $('#btn-journals-all').addEventListener('click', () => exportJournals(T.all.map(a => a.code)));
}
function showJournals(code) { journalsDisc = isAxis(code) ? code : members(code)[0]; journalsFollow = state.disc; renderJournals(); $('#revues').scrollIntoView({ behavior: 'smooth', block: 'start' }); }
function renderJournals() {
  if (journalsFollow !== state.disc) { journalsDisc = isAxis(state.disc) ? state.disc : members(state.disc)[0]; journalsFollow = state.disc; }
  if (!isAxis(journalsDisc)) journalsDisc = T.axes[0].code;
  const code = journalsDisc; $('#j-disc').value = code;
  const list = (D.topj || {})[code] || [], meta = ((D.topj_meta || {}).by_disc || {})[code] || {}, year = (D.topj_meta || {}).sjr_year;
  $('#journals-sub').innerHTML = `Les 10 revues les mieux classées par le SJR dans les catégories rattachées à <strong>${esc(code)} · ${esc(discLabel(code))}</strong>` + (year ? ` (classement ${year}, appliqué à toutes les années)` : '') + '. Chaque axe, disciplinaire ou transverse, a sa propre liste ; une macro-discipline additionne les articles parus dans les listes de ses axes.';
  const cats = meta.categories || [];
  $('#journals-cats').innerHTML = cats.length ? `<details class="app-cats"><summary>${cats.length} catégorie${cats.length > 1 ? 's' : ''} SJR (ASJC) utilisée${cats.length > 1 ? 's' : ''}</summary>${cats.map(c => `${c[0]} ${esc(c[1])}`).join(' · ')}</details>` : '';
  const tb = $('#journals-table tbody');
  if (!list.length) { tb.innerHTML = `<tr><td colspan="5" class="fr-text-mention--grey">${D.meta.demo ? 'Les listes de revues sont établies lors de l’extraction réelle : elles ne figurent pas dans le jeu de démonstration.' : 'Aucune revue retenue pour cette discipline.'}</td></tr>`; return; }
  tb.innerHTML = list.map((j, i) => `<tr><td class="num">${i + 1}</td>
    <td class="title">${esc(j.title)}${j.openalex_name && j.openalex_name.toLowerCase() !== j.title.toLowerCase() ? `<br><span class="fr-text--xs fr-text-mention--grey">OpenAlex : ${esc(j.openalex_name)}</span>` : ''}</td>
    <td class="num">${ok(j.sjr) ? nf2.format(j.sjr) : '–'}</td><td>${esc(j.issn || '')}</td>
    <td>${j.openalex_id ? `<a class="fr-link" href="${OA_URL(j.openalex_id)}" target="_blank" rel="noopener" title="${esc(j.title)} dans OpenAlex – nouvelle fenêtre">${esc(j.openalex_id)}</a>` : '<p class="fr-badge fr-badge--sm fr-badge--warning">Non trouvée par ISSN</p>'}</td></tr>`).join('');
}
function exportJournals(codes) {
  const head = ['Code discipline', 'Discipline', 'Rang', 'Revue (SJR)', 'Revue (OpenAlex)', 'SJR', 'ISSN', 'ID OpenAlex', 'Lien OpenAlex', 'Catégories SJR'];
  const rows = [];
  codes.forEach(c => {
    const cats = ((((D.topj_meta || {}).by_disc || {})[c] || {}).categories || []).map(x => x[0]).join(' ');
    ((D.topj || {})[c] || []).forEach((j, i) => rows.push([c, discLabel(c), i + 1, j.title, j.openalex_name || '', ok(j.sjr) ? String(j.sjr).replace('.', ',') : '', j.issn || '', j.openalex_id || '', j.openalex_id ? OA_URL(j.openalex_id) : '', cats]));
  });
  if (!rows.length) { alert('Aucune liste de revues disponible dans ces données.'); return; }
  downloadCsv([head, ...rows], codes.length === 1 ? `revues_reference_${codes[0]}.csv` : 'revues_reference_axes.csv');
}

/* ---------------------------------------------------------------- Topics par discipline (méthodologie) */
const TP_MAX = 400;
function topicsReady() { return D.topics && D.topics.by_axis; }
function buildTopics() {
  const sel = $('#tp-axis'); discOptions(sel, false);
  $$('#tp-axis optgroup option').forEach(o => { const n = topicsReady() ? (D.topics.by_axis[o.value] || []).length : 0; if (n) o.textContent += ` (${n} topics)`; });
  sel.addEventListener('change', renderTopics);
  let t; $('#tp-search').addEventListener('input', () => { clearTimeout(t); t = setTimeout(renderTopics, 150); });
  $('#tp-csv').addEventListener('click', () => exportTopics([$('#tp-axis').value]));
  $('#tp-csv-all').addEventListener('click', () => exportTopics(T.all.map(a => a.code), true));
  renderTopics();
}
const sfName = c => (D.topics.subfields || {})[c] || '';
const norm = x => String(x).toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');
function renderTopics() {
  const tb = $('#tp-table tbody'), th = $('#tp-table thead');
  if (!topicsReady()) {
    th.innerHTML = ''; $('#tp-sum').textContent = '';
    tb.innerHTML = `<tr><td class="fr-text-mention--grey">${D.meta.demo ? 'La liste des topics est établie lors de l’extraction réelle : elle ne figure pas dans le jeu de démonstration.' : 'Liste non disponible : régénérez le tableau de bord après une extraction (le cache des topics est nécessaire).'}</td></tr>`;
    $$('#tp-csv, #tp-csv-all, #tp-axis, #tp-search').forEach(b => (b.disabled = true)); return;
  }
  const q = norm($('#tp-search').value.trim()), axis = $('#tp-axis').value;
  const link = id => `<a class="fr-link fr-link--sm" href="https://openalex.org/${encodeURIComponent(id)}" target="_blank" rel="noopener" title="${esc(id)} dans OpenAlex – nouvelle fenêtre">${esc(id)}</a>`;
  const via = v => v === 'mc' ? '<p class="fr-badge fr-badge--sm fr-badge--info fr-badge--no-icon">mot-clé</p>' : '';
  if (q) {
    const hits = [];
    T.all.forEach(a => (D.topics.by_axis[a.code] || []).forEach(t => {
      if (norm(t[1]).includes(q) || norm(sfName(t[2])).includes(q) || norm(t[0]) === q || String(t[2]) === q) hits.push([a.code, ...t]);
    }));
    (D.topics.unassigned || []).forEach(t => { if (norm(t[1]).includes(q)) hits.push(['—', ...t]); });
    $('#tp-sum').innerHTML = `<strong>${fInt(hits.length)}</strong> rattachement${hits.length > 1 ? 's' : ''} trouvé${hits.length > 1 ? 's' : ''} pour « ${esc($('#tp-search').value.trim())} »${hits.length > TP_MAX ? ` (${TP_MAX} premiers affichés)` : ''}.`;
    th.innerHTML = '<tr><th scope="col">Axe</th><th scope="col">Topic</th><th scope="col">Sous-domaine</th><th scope="col">ID</th></tr>';
    tb.innerHTML = hits.slice(0, TP_MAX).map(h => `<tr><td><strong>${esc(h[0])}</strong>${isAxis(h[0]) ? ' ' + esc(truncate(T.axis[h[0]].label, 30)) : ' non rattaché'}</td><td>${esc(h[2])} ${via(h[4])}</td><td>${h[3]} · ${esc(sfName(h[3]))}</td><td>${link(h[1])}</td></tr>`).join('')
      || '<tr><td colspan="4" class="fr-text-mention--grey">Aucun topic.</td></tr>';
    return;
  }
  const list = D.topics.by_axis[axis] || [], nmc = list.filter(t => t[3] === 'mc').length;
  const groups = {}; list.forEach(t => (groups[t[2]] = groups[t[2]] || []).push(t));
  const sfs = Object.keys(groups).map(Number).sort((a, b) => (groups[a][0][3] === 'mc') - (groups[b][0][3] === 'mc') || a - b);
  const nsd = new Set(list.filter(t => t[3] === 'sd').map(t => t[2])).size;
  $('#tp-sum').innerHTML = `<strong>${esc(axis)} · ${esc(T.axis[axis].label)}</strong> : ${fInt(list.length)} topics`
    + (nsd ? `, issus de ${nsd} sous-domaine${nsd > 1 ? 's' : ''} rattachés en entier` : '')
    + (nmc ? (nsd ? `, plus <strong>${nmc}</strong> ajouté${nmc > 1 ? 's' : ''} par mot-clé` : `, tous ajoutés par mot-clé`) : '') + '.'
    + ` Sur l’ensemble, ${fInt(D.topics.n_topics || 0)} topics OpenAlex${(D.topics.unassigned || []).length ? `, dont ${D.topics.unassigned.length} rattachés à aucun axe disciplinaire` : ', tous rattachés à au moins un axe'}.`;
  th.innerHTML = '<tr><th scope="col">Topic</th><th scope="col">ID OpenAlex</th></tr>';
  tb.innerHTML = sfs.map(sf => {
    const g = groups[sf], mc = g.every(t => t[3] === 'mc');
    return `<tr class="is-parent"><td colspan="2">${sf} · ${esc(sfName(sf))} <span class="fr-text--xs fr-text-mention--grey">— ${g.length} topic${g.length > 1 ? 's' : ''}${mc ? ', ajoutés par mot-clé' : ''}</span></td></tr>`
      + g.map(t => `<tr><td>${esc(t[1])} ${via(t[3])}</td><td>${link(t[0])}</td></tr>`).join('');
  }).join('') || '<tr><td colspan="2" class="fr-text-mention--grey">Aucun topic.</td></tr>';
}
function exportTopics(codes, withUnassigned = false) {
  if (!topicsReady()) return;
  const head = ['Code axe', 'Axe', 'Macro-discipline', 'Topic ID', 'Topic', 'Sous-domaine (ASJC)', 'Sous-domaine', 'Rattachement', 'Lien OpenAlex'];
  const rows = [];
  codes.forEach(c => (D.topics.by_axis[c] || []).forEach(t => rows.push([c, T.axis[c].label, macroLabel(c), t[0], t[1], t[2], sfName(t[2]),
    t[3] === 'mc' ? 'mot-clé' : 'sous-domaine', `https://openalex.org/${t[0]}`])));
  if (withUnassigned) (D.topics.unassigned || []).forEach(t => rows.push(['', 'non rattaché', '', t[0], t[1], t[2], sfName(t[2]), '', `https://openalex.org/${t[0]}`]));
  downloadCsv([head, ...rows], codes.length === 1 ? `topics_${codes[0]}.csv` : 'topics_axes.csv');
}

/* ======================================================================
   Connexion / démarrage
   ====================================================================== */
async function openApp(dekRaw, login) {
  const data = await decryptData(dekRaw);
  indexData(data); buildFilters(); buildJournalsSelect(); buildTopics();
  resetState(); readHash(); syncControls();
  $('#login-view').hidden = true; $('#app-view').hidden = false;
  document.body.classList.add('is-auth');
  $$('.app-user-name').forEach(n => (n.textContent = login || ''));
  $('#demo-notice').hidden = !data.meta.demo;
  fillMethod();
  requestAnimationFrame(render);
}
function fillMethod() {
  const set = (sel, v) => $$(sel).forEach(n => (n.textContent = v));
  set('#m-ce', CE);
  set('#m-n-eu', PANEL.eu.length); set('.m-n-eu1', PANEL.eu.length + 1); set('.m-n-all', PANEL.all.length);
  set('#m-panel-eu', PANEL.eu.map(id => E[id].label).join(', '));
  set('#m-n-world', PANEL.world.length);
  set('#m-panel-world', PANEL.world.map(id => E[id].label).join(', '));
  const wy = (D.meta.panels || {}).monde_annees; set('#m-world-years', wy ? `${wy[0]}–${wy[1]}` : 'les dernières années');
  const ma = D.meta.max_auteurs;
  set('#m-maxaut', ma ? `Les articles de plus de ${ma} auteurs (très grandes collaborations) sont exclus de tous les comptages.` : '');
  set('#m-maxaut2', ma ? `Ici, les articles de plus de ${ma} auteurs sont exclus.` : 'Option possible : exclure les articles de plus de N auteurs (paramètre max_auteurs de l’extraction).');
}
function logout() { sessionSet(null); location.hash = ''; location.reload(); }
async function onLogin(ev) {
  ev.preventDefault();
  const login = $('#login').value, pwd = $('#password').value, btn = $('#login-btn'), err = $('#login-error');
  err.hidden = true;
  if (!login || !pwd) { $('#login-error-text').textContent = 'Saisissez votre identifiant et votre mot de passe.'; err.hidden = false; return; }
  btn.disabled = true; btn.textContent = 'Vérification…';
  try {
    const dek = await unwrapKey(login, pwd);
    sessionSet({ k: b64e(dek), u: login.trim() }); $('#password').value = '';
    await openApp(dek, login.trim());
  } catch (e) {
    $('#login-error-text').textContent = e.message === 'auth' ? 'Identifiant ou mot de passe incorrect.' : 'Erreur lors du déchiffrement : ' + e.message;
    err.hidden = false;
  } finally { btn.disabled = false; btn.textContent = 'Se connecter'; }
}
async function boot() {
  $('#login-form').addEventListener('submit', onLogin);
  document.addEventListener('click', e => {
    if (e.target.closest('.app-logout')) logout();
    const om = e.target.closest('[data-open-method]');
    if (om) { e.preventDefault(); const b = $('[aria-controls="acc-method"]'); if (b && b.getAttribute('aria-expanded') !== 'true') b.click();
      setTimeout(() => $('#acc-method').scrollIntoView({ behavior: 'smooth', block: 'start' }), 150); }
    const jl = e.target.closest('[data-journals]'); if (jl) { e.preventDefault(); showJournals(jl.dataset.journals); }
  });
  bindFilters();
  if (!window.crypto || !crypto.subtle || typeof DecompressionStream === 'undefined') {
    $('#login-error-text').textContent = 'Navigateur non compatible : utilisez une version récente de Firefox, Chrome, Edge ou Safari.';
    $('#login-error').hidden = false; $('#login-btn').disabled = true; return;
  }
  const s = sessionGet();
  if (s && s.k) { try { await openApp(b64d(s.k), s.u); return; } catch (e) { sessionSet(null); } }
  $('#login').focus();
}
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
})();

