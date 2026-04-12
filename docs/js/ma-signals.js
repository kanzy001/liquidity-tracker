// ============================================================
// S&P 500 MA SIGNAL ENGINE
// Drop this file into your repo as: js/ma-signals.js
// Then add <script src="js/ma-signals.js"></script> to index.html
// and add <section id="ma-signals-section"></section> where you
// want the panel to appear.
// ============================================================

(function () {

  // ----------------------------------------------------------
  // MATH HELPERS
  // ----------------------------------------------------------

  function sma(arr, n) {
    const out = new Array(arr.length).fill(null);
    for (let i = n - 1; i < arr.length; i++) {
      let s = 0;
      for (let j = 0; j < n; j++) s += arr[i - j];
      out[i] = s / n;
    }
    return out;
  }

  function ema(arr, n) {
    const k = 2 / (n + 1);
    const out = new Array(arr.length).fill(null);
    let started = false;
    for (let i = 0; i < arr.length; i++) {
      if (arr[i] == null) continue;
      if (!started) { out[i] = arr[i]; started = true; continue; }
      const prev = out[i - 1] != null ? out[i - 1] : arr[i];
      out[i] = arr[i] * k + prev * (1 - k);
    }
    return out;
  }

  function pctGap(a, b) {
    return ((a - b) / b * 100).toFixed(2);
  }

  function f(n) {
    return n != null ? n.toFixed(2) : '—';
  }

  // ----------------------------------------------------------
  // SIGNAL COMPUTATION
  // ----------------------------------------------------------

  function computeSignals(closes) {
    const s7   = sma(closes, 7);
    const s25  = sma(closes, 25);
    const s50  = sma(closes, 50);
    const s99  = sma(closes, 99);
    const s200 = sma(closes, 200);
    const s305 = sma(closes, 305);
    const s610 = sma(closes, 610);
    const e9   = ema(closes, 9);
    const e21  = ema(closes, 21);
    const e80  = ema(closes, 80);

    const i    = closes.length - 1;
    const prev = i - 1;

    // ---- Golden / Death Cross ----
    const cur50 = s50[i], cur200 = s200[i];
    const prv50 = s50[prev], prv200 = s200[prev];
    const trending50up = s50[i] > s50[Math.max(0, i - 5)];

    let crossLabel, crossStatus;
    if (cur50 > cur200 && prv50 <= prv200) {
      crossLabel = 'Golden Cross firing'; crossStatus = 'bull';
    } else if (cur50 < cur200 && prv50 >= prv200) {
      crossLabel = 'Death Cross firing'; crossStatus = 'bear';
    } else if (cur50 > cur200) {
      crossLabel = trending50up ? 'Above 200 · SMA50 rising' : 'Above 200 · SMA50 flattening';
      crossStatus = trending50up ? 'bull' : 'warn';
    } else {
      crossLabel = trending50up ? 'Below 200 · SMA50 recovering' : 'Below 200 · SMA50 declining';
      crossStatus = 'bear';
    }

    // ---- Bullish / Bearish Stack ----
    const cur7 = s7[i], cur25 = s25[i], cur99 = s99[i];
    let stackLabel, stackStatus;
    if (cur7 > cur25 && cur25 > cur99) {
      stackLabel = 'Bullish Stack (7 > 25 > 99)'; stackStatus = 'bull';
    } else if (cur7 < cur25 && cur25 < cur99) {
      stackLabel = 'Bearish Stack (7 < 25 < 99)'; stackStatus = 'bear';
    } else {
      stackLabel = 'Mixed — no clear stack'; stackStatus = 'warn';
    }

    // ---- Reversal Zone ----
    const curE21 = e21[i], curS200 = s200[i];
    const revDiffPct = Math.abs(curE21 - curS200) / curS200 * 100;
    const inRevZone  = revDiffPct <= 1.5;
    const revLabel   = inRevZone ? 'Reversal zone active' : 'Outside reversal zone';
    const revStatus  = inRevZone ? 'warn' : (curE21 > curS200 ? 'bull' : 'bear');

    // ---- Stability / Confluence ----
    let stabLabel, stabStatus, stabMeta;
    const hasLongHistory = s610[i] != null;
    if (hasLongHistory) {
      const confluence = (s200[i] + s305[i] + s610[i]) / 3;
      const devs = [e9[i], e21[i], e80[i]].map(v => Math.abs(v - confluence) / confluence * 100);
      const allInZone = devs.every(d => d < 3.0);
      stabLabel  = allInZone ? 'EMAs inside confluence zone' : 'EMAs outside confluence zone';
      stabStatus = allInZone ? 'warn' : (e9[i] > confluence ? 'bull' : 'bear');
      stabMeta   = {
        confluence: confluence.toFixed(2),
        devE9:  devs[0].toFixed(2),
        devE21: devs[1].toFixed(2),
        devE80: devs[2].toFixed(2),
      };
    } else {
      stabLabel  = 'Need 610 days of history';
      stabStatus = 'neu';
      stabMeta   = null;
    }

    return {
      price:   closes[i],
      date:    null, // filled in by caller
      cross:   { label: crossLabel, status: crossStatus, sma50: cur50, sma200: cur200, gap: pctGap(cur50, cur200), trending50up },
      stack:   { label: stackLabel, status: stackStatus, s7: cur7, s25: cur25, s99: cur99 },
      rev:     { label: revLabel, status: revStatus, ema21: curE21, sma200: curS200, diffPct: revDiffPct.toFixed(2) },
      stab:    { label: stabLabel, status: stabStatus, meta: stabMeta },
      series:  { s50, s200, e21, closes },
    };
  }

  // ----------------------------------------------------------
  // FETCH PRICE DATA  (Yahoo Finance — no key required)
  // ----------------------------------------------------------

  async function fetchSP500() {
    const url = 'https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1d&range=3y';
    const res  = await fetch(url);
    if (!res.ok) throw new Error('Yahoo Finance fetch failed: ' + res.status);
    const json = await res.json();
    const result = json.chart.result[0];
    const rawCloses    = result.indicators.quote[0].close;
    const timestamps   = result.timestamp;
    // strip nulls (market holidays produce nulls)
    const closes = rawCloses.filter(v => v != null);
    const lastTs = timestamps[timestamps.length - 1];
    const lastDate = new Date(lastTs * 1000).toLocaleDateString('en-US', {
      weekday: 'short', year: 'numeric', month: 'short', day: 'numeric'
    });
    return { closes, lastDate };
  }

  // ----------------------------------------------------------
  // RENDER HTML
  // ----------------------------------------------------------

  function statusClass(s) {
    return { bull: 'signal-bull', bear: 'signal-bear', warn: 'signal-warn', neu: 'signal-neu' }[s] || 'signal-neu';
  }
  function statusWord(s) {
    return { bull: 'Bullish', bear: 'Bearish', warn: 'Caution', neu: 'Neutral' }[s] || 'Neutral';
  }

  function buildHTML(sig) {
    const cards = [
      {
        name: 'Golden / Death Cross',
        sub:  'SMA 50 vs SMA 200',
        label: sig.cross.label,
        status: sig.cross.status,
        meta: [
          { k: 'SMA50',  v: f(sig.cross.sma50)  },
          { k: 'SMA200', v: f(sig.cross.sma200) },
          { k: 'Gap',    v: sig.cross.gap + '%'  },
        ],
        desc: 'Primary trend signal. Golden = SMA50 crosses above SMA200 (risk-on). Death = inverse (risk-off).',
      },
      {
        name: 'Bullish / Bearish Stack',
        sub:  'SMA 7 / 25 / 99 triple alignment',
        label: sig.stack.label,
        status: sig.stack.status,
        meta: [
          { k: 'SMA7',  v: f(sig.stack.s7)  },
          { k: 'SMA25', v: f(sig.stack.s25) },
          { k: 'SMA99', v: f(sig.stack.s99) },
        ],
        desc: 'Bullish = 7 > 25 > 99 (momentum expanding). Bearish = 7 < 25 < 99 (momentum contracting).',
      },
      {
        name: 'Reversal Zone',
        sub:  'EMA21 within 1.5% of SMA200',
        label: sig.rev.label,
        status: sig.rev.status,
        meta: [
          { k: 'EMA21',    v: f(sig.rev.ema21)   },
          { k: 'SMA200',   v: f(sig.rev.sma200)  },
          { k: 'Distance', v: sig.rev.diffPct + '%' },
        ],
        desc: 'EMA21 compressing to SMA200 = mean-reversion tension. Potential direction change approaching.',
      },
      {
        name: 'Stability / Confluence',
        sub:  'EMA 9/21/80 near SMA 200/305/610',
        label: sig.stab.label,
        status: sig.stab.status,
        meta: sig.stab.meta ? [
          { k: 'Confluence', v: sig.stab.meta.confluence },
          { k: 'EMA9 dev',   v: sig.stab.meta.devE9 + '%'  },
          { k: 'EMA21 dev',  v: sig.stab.meta.devE21 + '%' },
          { k: 'EMA80 dev',  v: sig.stab.meta.devE80 + '%' },
        ] : [{ k: 'Note', v: 'Need 610 trading days (~2.5 yrs)' }],
        desc: 'All three EMAs inside the long-MA confluence zone signals low-volatility stability / compression before a move.',
      },
    ];

    const regimeColor = { bull: '#4ade80', bear: '#f87171', warn: '#fbbf24', neu: '#9ca3af' }[sig.cross.status];
    const regimeWord  = { bull: 'Risk-On', bear: 'Risk-Off', warn: 'Transitional', neu: 'Neutral' }[sig.cross.status];

    let html = `
<div class="mas-section">
  <div class="mas-header">
    <div>
      <div class="mas-title">S&amp;P 500 · MA Signal Dashboard</div>
      <div class="mas-sub">Golden/Death Cross · Stack · Reversal · Stability · live from Yahoo Finance</div>
    </div>
    <div class="mas-header-right">
      <span class="mas-badge mas-badge-live" id="mas-status-badge">Live · Yahoo Finance</span>
      <button class="mas-refresh" onclick="window.MASignals.reload()">↻ Refresh</button>
    </div>
  </div>

  <div class="mas-price-row">
    <div class="mas-pstat">
      <div class="mas-pstat-label">S&amp;P 500</div>
      <div class="mas-pstat-val">${sig.price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
    </div>
    <div class="mas-pstat">
      <div class="mas-pstat-label">As of</div>
      <div class="mas-pstat-val" style="font-size:13px">${sig.date}</div>
    </div>
    <div class="mas-pstat">
      <div class="mas-pstat-label">Regime</div>
      <div class="mas-pstat-val" style="font-size:14px;color:${regimeColor}">${regimeWord}</div>
    </div>
  </div>

  <div class="mas-grid">`;

    for (const c of cards) {
      html += `
    <div class="mas-card">
      <div class="mas-card-top">
        <div>
          <div class="mas-card-name">${c.name}</div>
          <div class="mas-card-sub">${c.sub}</div>
        </div>
        <span class="mas-pill ${statusClass(c.status)}">${statusWord(c.status)}</span>
      </div>
      <div class="mas-card-label">${c.label}</div>
      <div class="mas-card-desc">${c.desc}</div>
      <div class="mas-card-meta">
        ${c.meta.map(m => `<span class="mas-mv"><span class="mas-mk">${m.k}</span>${m.v}</span>`).join('')}
      </div>
    </div>`;
    }

    html += `
  </div>

  <div class="mas-chart-wrap">
    <div class="mas-chart-head">
      <span class="mas-chart-title">Price · SMA50 · SMA200 · EMA21</span>
      <div class="mas-legend">
        <span class="mas-leg"><span style="background:#e8e2d4"></span>Price</span>
        <span class="mas-leg"><span style="background:#60a5fa"></span>SMA50</span>
        <span class="mas-leg"><span style="background:#f87171"></span>SMA200</span>
        <span class="mas-leg"><span style="background:#a78bfa;border-style:dashed"></span>EMA21</span>
      </div>
    </div>
    <div style="position:relative;height:260px">
      <canvas id="mas-chart" role="img" aria-label="S&P 500 price with SMA50, SMA200, EMA21 overlay">S&P 500 moving average chart.</canvas>
    </div>
  </div>

  <div class="mas-note">
    Data: Yahoo Finance (^GSPC) · All MAs computed client-side from daily closes ·
    SMA610 requires ~2.5 years of history · Not financial advice
  </div>
</div>`;

    return html;
  }

  // ----------------------------------------------------------
  // CHART
  // ----------------------------------------------------------

  let chartInstance = null;

  function drawChart(sig) {
    const N    = sig.series.closes.length;
    const step = Math.max(1, Math.floor(N / 120));
    const labels = [], cp = [], cs50 = [], cs200 = [], ce21 = [];

    for (let i = 0; i < N; i += step) {
      const d = new Date(Date.now() - (N - i) * 86400000);
      labels.push(d.toLocaleDateString('en-US', { month: 'short', year: '2-digit' }));
      cp.push(sig.series.closes[i]);
      cs50.push(sig.series.s50[i]);
      cs200.push(sig.series.s200[i]);
      ce21.push(sig.series.e21[i]);
    }

    if (chartInstance) { chartInstance.destroy(); chartInstance = null; }

    // Chart.js must already be loaded on the page (your tracker likely has it; if not add the CDN tag)
    if (typeof Chart === 'undefined') {
      document.getElementById('mas-chart').parentElement.innerHTML =
        '<div style="color:#6b7280;font-size:12px;padding:20px">Chart.js not loaded — add ' +
        '&lt;script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"&gt;&lt;/script&gt; to your page.</div>';
      return;
    }

    chartInstance = new Chart(document.getElementById('mas-chart'), {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Price',  data: cp,   borderColor: '#e8e2d4', borderWidth: 1.5, pointRadius: 0, tension: 0.2 },
          { label: 'SMA50',  data: cs50,  borderColor: '#60a5fa', borderWidth: 1.2, pointRadius: 0, tension: 0.2 },
          { label: 'SMA200', data: cs200, borderColor: '#f87171', borderWidth: 1.8, pointRadius: 0, tension: 0.2 },
          { label: 'EMA21',  data: ce21,  borderColor: '#a78bfa', borderWidth: 1,   pointRadius: 0, tension: 0.2, borderDash: [4, 3] },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: {
            ticks: { color: '#4b5563', font: { size: 10 }, maxTicksLimit: 10 },
            grid:  { color: 'rgba(255,255,255,0.04)' },
          },
          y: {
            ticks: { color: '#4b5563', font: { size: 10 }, callback: v => v ? v.toLocaleString() : '' },
            grid:  { color: 'rgba(255,255,255,0.05)' },
          },
        },
      },
    });
  }

  // ----------------------------------------------------------
  // INJECT CSS  (matches your tracker's dark theme)
  // ----------------------------------------------------------

  function injectStyles() {
    if (document.getElementById('mas-styles')) return;
    const s = document.createElement('style');
    s.id = 'mas-styles';
    s.textContent = `
.mas-section{background:#0d1117;padding:24px 0;border-top:1px solid rgba(255,255,255,0.07);margin-top:32px}
.mas-header{display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;margin-bottom:18px}
.mas-title{font-size:18px;font-weight:600;color:#f8f4ec;letter-spacing:-.01em}
.mas-sub{font-size:11px;color:#6b7280;margin-top:3px}
.mas-header-right{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.mas-badge{font-size:10px;padding:3px 9px;border-radius:4px;font-weight:500;letter-spacing:.05em}
.mas-badge-live{background:rgba(74,222,128,.12);border:1px solid rgba(74,222,128,.3);color:#4ade80}
.mas-badge-err{background:rgba(248,113,113,.12);border:1px solid rgba(248,113,113,.3);color:#f87171}
.mas-refresh{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.1);color:#9ca3af;padding:5px 12px;border-radius:5px;cursor:pointer;font-size:11px}
.mas-refresh:hover{background:rgba(255,255,255,.09);color:#e8e2d4}
.mas-price-row{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px}
.mas-pstat{background:#111827;border:1px solid rgba(255,255,255,.07);border-radius:7px;padding:10px 14px;flex:1;min-width:100px}
.mas-pstat-label{font-size:10px;color:#6b7280;margin-bottom:4px;text-transform:uppercase;letter-spacing:.06em}
.mas-pstat-val{font-size:18px;font-weight:600;color:#f8f4ec}
.mas-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-bottom:16px}
.mas-card{background:#111827;border:1px solid rgba(255,255,255,.07);border-radius:8px;padding:14px 16px}
.mas-card-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px}
.mas-card-name{font-size:12px;font-weight:500;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em}
.mas-card-sub{font-size:10px;color:#4b5563;margin-top:2px}
.mas-pill{font-size:11px;font-weight:600;padding:2px 9px;border-radius:4px;white-space:nowrap}
.signal-bull{background:rgba(74,222,128,.15);color:#4ade80}
.signal-bear{background:rgba(248,113,113,.15);color:#f87171}
.signal-warn{background:rgba(251,191,36,.15);color:#fbbf24}
.signal-neu{background:rgba(107,114,128,.15);color:#9ca3af}
.mas-card-label{font-size:15px;font-weight:600;color:#f8f4ec;margin-bottom:6px}
.mas-card-desc{font-size:11px;color:#6b7280;line-height:1.55;margin-bottom:8px}
.mas-card-meta{display:flex;flex-wrap:wrap;gap:8px}
.mas-mv{font-size:10px;color:#9ca3af}
.mas-mk{color:#4b5563;margin-right:3px}
.mas-chart-wrap{background:#111827;border:1px solid rgba(255,255,255,.07);border-radius:8px;padding:14px 16px;margin-bottom:12px}
.mas-chart-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px}
.mas-chart-title{font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em}
.mas-legend{display:flex;flex-wrap:wrap;gap:12px}
.mas-leg{display:flex;align-items:center;gap:5px;font-size:10px;color:#6b7280}
.mas-leg span{display:inline-block;width:18px;height:2px;border-radius:1px}
.mas-note{font-size:10px;color:#374151;line-height:1.7;padding:8px 12px;background:rgba(255,255,255,.02);border:1px solid rgba(255,255,255,.04);border-radius:6px}
    `;
    document.head.appendChild(s);
  }

  // ----------------------------------------------------------
  // MAIN LOAD FUNCTION
  // ----------------------------------------------------------

  async function load() {
    const container = document.getElementById('ma-signals-section');
    if (!container) {
      console.warn('[MASignals] No element with id="ma-signals-section" found.');
      return;
    }

    injectStyles();
    container.innerHTML = '<div style="padding:32px;text-align:center;color:#6b7280;font-size:13px">Loading S&P 500 price data…</div>';

    const badge = document.getElementById('mas-status-badge');

    try {
      const { closes, lastDate } = await fetchSP500();

      if (closes.length < 200) throw new Error('Fewer than 200 data points returned');

      const sig  = computeSignals(closes);
      sig.date   = lastDate;

      container.innerHTML = buildHTML(sig);
      drawChart(sig);

      const b = document.getElementById('mas-status-badge');
      if (b) { b.textContent = 'Live · Yahoo Finance'; b.className = 'mas-badge mas-badge-live'; }

    } catch (err) {
      console.error('[MASignals]', err);
      container.innerHTML = `
        <div style="background:rgba(248,113,113,.08);border:1px solid rgba(248,113,113,.2);border-radius:8px;padding:20px;color:#f87171;font-size:12px;text-align:center">
          Could not load S&P 500 data: ${err.message}<br>
          <button onclick="window.MASignals.reload()" style="margin-top:10px;background:rgba(248,113,113,.15);border:1px solid #f87171;color:#f87171;padding:5px 14px;border-radius:5px;cursor:pointer;font-size:11px">Retry</button>
        </div>`;
    }
  }

  // ----------------------------------------------------------
  // PUBLIC API
  // ----------------------------------------------------------

  window.MASignals = { reload: load };

  // Auto-init when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', load);
  } else {
    load();
  }

})();
