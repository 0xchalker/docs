/* ============================================================
   Funding Scalp Bot Dashboard - Frontend Logic
   app.js  (Alpine.js component + Chart.js + WebSocket)
   ============================================================ */

'use strict';

/* ----------------------------------------------------------
   WebSocket Connection Manager
   ---------------------------------------------------------- */
class BotWebSocket {
  constructor(url, onMessage, onStatusChange) {
    this._url = url;
    this._onMessage = onMessage;
    this._onStatusChange = onStatusChange;
    this._ws = null;
    this._reconnectTimer = null;
    this._intentionalClose = false;
    this._reconnectDelay = 2000;
  }

  connect() {
    this._intentionalClose = false;
    this._doConnect();
  }

  disconnect() {
    this._intentionalClose = true;
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this._ws) {
      this._ws.close();
      this._ws = null;
    }
    this._onStatusChange && this._onStatusChange(false);
  }

  _doConnect() {
    try {
      this._ws = new WebSocket(this._url);
    } catch (e) {
      console.warn('[WS] Failed to create WebSocket:', e);
      this._scheduleReconnect();
      return;
    }

    this._ws.onopen = () => {
      console.log('[WS] Connected');
      this._reconnectDelay = 2000;
      this._onStatusChange && this._onStatusChange(true);
    };

    this._ws.onmessage = (evt) => {
      try {
        const data = JSON.parse(evt.data);
        this._onMessage && this._onMessage(data);
      } catch (e) {
        console.warn('[WS] Bad JSON:', e);
      }
    };

    this._ws.onclose = () => {
      this._onStatusChange && this._onStatusChange(false);
      if (!this._intentionalClose) {
        this._scheduleReconnect();
      }
    };

    this._ws.onerror = (e) => {
      console.warn('[WS] Error:', e);
    };
  }

  _scheduleReconnect() {
    if (this._intentionalClose) return;
    console.log(`[WS] Reconnecting in ${this._reconnectDelay}ms…`);
    this._reconnectTimer = setTimeout(() => {
      this._reconnectDelay = Math.min(this._reconnectDelay * 1.5, 30000);
      this._doConnect();
    }, this._reconnectDelay);
  }
}

/* ----------------------------------------------------------
   Formatters
   ---------------------------------------------------------- */
const fmt = {
  usd(val) {
    if (val == null) return '—';
    const n = parseFloat(val);
    if (isNaN(n)) return '—';
    const abs = Math.abs(n);
    let s;
    if (abs >= 1e6) s = (n / 1e6).toFixed(2) + 'M';
    else if (abs >= 1e3) s = (n / 1e3).toFixed(2) + 'K';
    else s = n.toFixed(2);
    return (n >= 0 ? '+' : '') + '$' + s.replace('-', '');
  },

  usdPlain(val) {
    if (val == null) return '—';
    const n = parseFloat(val);
    if (isNaN(n)) return '—';
    return '$' + Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  },

  pct(val, decimals = 2) {
    if (val == null) return '—';
    const n = parseFloat(val);
    if (isNaN(n)) return '—';
    return n.toFixed(decimals) + '%';
  },

  bps(val) {
    if (val == null) return '—';
    return parseFloat(val).toFixed(1) + ' bps';
  },

  oi(val) {
    if (val == null) return '—';
    const n = parseFloat(val);
    if (n >= 1e9) return '$' + (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return '$' + (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return '$' + (n / 1e3).toFixed(0) + 'K';
    return '$' + n.toFixed(0);
  },

  holdTime(secs) {
    if (secs == null || isNaN(secs)) return '—';
    const s = Math.round(secs);
    if (s < 60) return s + 's';
    const m = Math.floor(s / 60);
    const rem = s % 60;
    return `${m}m ${rem}s`;
  },

  countdown(secs) {
    if (secs == null || isNaN(secs)) return '--:--';
    const total = Math.max(0, Math.round(secs));
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const s = total % 60;
    if (h > 0) return `${h}h ${String(m).padStart(2, '0')}m`;
    return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  },

  ratio(val) {
    if (val == null) return '—';
    return parseFloat(val).toFixed(3);
  },

  price(val) {
    if (val == null) return '—';
    const n = parseFloat(val);
    if (n >= 1000) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return n.toFixed(4);
  },

  score(val) {
    if (val == null) return '0';
    return String(parseInt(val));
  },

  datetime(str) {
    if (!str) return '—';
    try {
      const d = new Date(str);
      return d.toLocaleString('en-US', {
        month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: false,
      });
    } catch {
      return str;
    }
  },

  timeOnly(str) {
    if (!str) return '—';
    try {
      return new Date(str).toLocaleTimeString('en-US', { hour12: false });
    } catch {
      return str;
    }
  },
};

/* ----------------------------------------------------------
   Helpers
   ---------------------------------------------------------- */
function fundingClass(pct) {
  const n = parseFloat(pct);
  if (isNaN(n)) return '';
  if (n <= -1.0) return 'funding-extreme';
  if (n <= -0.5) return 'funding-high';
  if (n <= -0.2) return 'funding-mid';
  if (n < 0) return 'funding-low';
  return 'funding-pos';
}

function scoreColor(score, maxScore) {
  const pct = (score / maxScore) * 100;
  if (pct >= 75) return 'badge-green';
  if (pct >= 55) return 'badge-yellow';
  return 'badge-red';
}

function decisionBadgeClass(decision) {
  if (!decision) return 'decision-none';
  const d = decision.toUpperCase();
  if (d.includes('LONG')) return 'decision-long';
  if (d.includes('SHORT')) return 'decision-short';
  return 'decision-none';
}

function decisionLabel(decision) {
  if (!decision) return 'NO TRADE —';
  const d = decision.toUpperCase();
  if (d.includes('LONG')) return 'LONG ↑';
  if (d.includes('SHORT')) return 'SHORT ↓';
  return 'NO TRADE —';
}

function symbolGroup(symbol) {
  if (!symbol) return 'ALT';
  const s = symbol.toUpperCase();
  if (s.startsWith('BTC')) return 'BTC';
  if (s.startsWith('ETH')) return 'ETH';
  return 'ALT';
}

function symbolGroupBadge(symbol) {
  const g = symbolGroup(symbol);
  if (g === 'BTC') return 'badge-yellow';
  if (g === 'ETH') return 'badge-blue';
  return 'badge-purple';
}

function stateColorClass(state) {
  if (!state) return 'badge-gray';
  const s = state.toUpperCase();
  const map = {
    FLAT: 'badge-gray',
    WARMUP: 'badge-blue',
    PRE_FUNDING_SCAN: 'badge-blue',
    SCAN: 'badge-blue',
    ARMED: 'badge-yellow',
    DECISION_LOCKED: 'badge-yellow',
    LOCKED: 'badge-yellow',
    ENTERING: 'badge-purple',
    IN_POSITION: 'badge-green',
    EXITING: 'badge-red',
    COOLDOWN: 'badge-red',
  };
  return map[s] || 'badge-gray';
}

const ALL_STATES = [
  'FLAT',
  'WARMUP',
  'PRE_FUNDING_SCAN',
  'ARMED',
  'DECISION_LOCKED',
  'ENTERING',
  'IN_POSITION',
  'EXITING',
  'COOLDOWN',
];

/* ----------------------------------------------------------
   Ring SVG circumference for countdown
   ---------------------------------------------------------- */
const RING_R = 30;
const RING_C = 2 * Math.PI * RING_R;  // ~188.5

function ringDashOffset(secondsLeft, totalSeconds = 28800 /* 8h */) {
  const frac = Math.max(0, Math.min(1, secondsLeft / totalSeconds));
  return RING_C * (1 - frac);
}

function ringUrgencyClass(secondsLeft) {
  if (secondsLeft <= 60) return 'critical';
  if (secondsLeft <= 300) return 'urgent';
  return '';
}

/* ----------------------------------------------------------
   Alpine.js main component
   ---------------------------------------------------------- */
function botDashboard() {
  return {
    /* ---- state ---- */
    connected: false,
    status: { bot_running: false, dry_run: true, symbols: [], states: {}, uptime_seconds: 0 },
    scanner: { candidates: [], scanned_at: null },
    signals: { signals: {} },
    positions: { positions: [] },
    trades: { trades: [], total: 0 },
    metrics: {
      overall: { total_trades: 0, win_rate: 0, profit_factor: 0, expectancy_r: 0, total_pnl_usd: 0, max_drawdown_usd: 0, avg_hold_seconds: 0 },
      today: { trades: 0, pnl_usd: 0, wins: 0, losses: 0 },
      by_mode: { LONG_AT_ROLLOVER: {}, SHORT_AT_ROLLOVER: {} },
      pnl_series: [],
    },
    config: {},

    /* ---- live timing ---- */
    countdown: {},       // { BTCUSDT: 1234.5, ... }
    wsStates: {},        // { BTCUSDT: "FLAT", ... }
    lastUpdate: null,

    /* ---- chart ---- */
    chart: null,
    chartMode: 'cumulative',

    /* ---- pagination ---- */
    tradePage: 0,
    tradePageSize: 25,
    tradeFilter: { symbol: '', mode: '' },

    /* ---- ws manager ---- */
    _ws: null,
    _countdownInterval: null,

    /* ================================================================
       init
       ================================================================ */
    async init() {
      await this.fetchAll();
      this.initChart();
      this.startWebSocket();
      // Tick countdowns locally every second for smooth display
      this._countdownInterval = setInterval(() => this.tickCountdowns(), 1000);
    },

    /* ================================================================
       Data fetching
       ================================================================ */
    async fetchAll() {
      await Promise.allSettled([
        this.fetchStatus(),
        this.fetchScanner(),
        this.fetchSignals(),
        this.fetchPositions(),
        this.fetchTrades(),
        this.fetchMetrics(),
        this.fetchConfig(),
      ]);
      this.lastUpdate = new Date();
    },

    async _get(url) {
      try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return await res.json();
      } catch (e) {
        console.warn(`[API] GET ${url} failed:`, e);
        return null;
      }
    },

    async fetchStatus() {
      const d = await this._get('/api/status');
      if (d) {
        this.status = d;
        // merge states into wsStates if WS hasn't given us any
        if (!Object.keys(this.wsStates).length) {
          this.wsStates = { ...(d.states || {}) };
        }
      }
    },

    async fetchScanner() {
      const d = await this._get('/api/scanner');
      if (d) this.scanner = d;
    },

    async fetchSignals() {
      const d = await this._get('/api/signals');
      if (d) this.signals = d;
    },

    async fetchPositions() {
      const d = await this._get('/api/positions');
      if (d) this.positions = d;
    },

    async fetchTrades() {
      const { symbol, mode } = this.tradeFilter;
      const offset = this.tradePage * this.tradePageSize;
      const params = new URLSearchParams({
        limit: this.tradePageSize,
        page: this.tradePage,
        symbol: symbol || '',
      });
      if (mode) params.set('mode', mode);
      const d = await this._get(`/api/trades?${params}`);
      if (d) this.trades = d;
    },

    async fetchMetrics() {
      const d = await this._get('/api/metrics');
      if (d) {
        this.metrics = d;
        this.updateChart();
      }
    },

    async fetchConfig() {
      const d = await this._get('/api/config');
      if (d) this.config = d;
    },

    /* ================================================================
       WebSocket
       ================================================================ */
    startWebSocket() {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      const url = `${proto}://${location.host}/ws/live`;
      this._ws = new BotWebSocket(
        url,
        (data) => this.handleWsMessage(data),
        (connected) => { this.connected = connected; }
      );
      this._ws.connect();
    },

    handleWsMessage(data) {
      const type = data.type;

      if (type === 'tick') {
        if (data.states) this.wsStates = data.states;
        if (data.seconds_to_next_funding) this.countdown = data.seconds_to_next_funding;
        if (data.active_signals && Object.keys(data.active_signals).length) {
          this.signals = { signals: data.active_signals };
        }
        this.lastUpdate = new Date();
      } else if (type === 'trade_opened' || type === 'trade_closed') {
        this.fetchPositions();
        this.fetchTrades();
        this.fetchMetrics();
      } else if (type === 'scan_update') {
        this.fetchScanner();
      } else if (type === 'state_change') {
        this.fetchStatus();
      }
    },

    /* ================================================================
       Countdown ticking (local interpolation between WS ticks)
       ================================================================ */
    tickCountdowns() {
      for (const sym in this.countdown) {
        if (this.countdown[sym] > 0) {
          this.countdown[sym] = Math.max(0, this.countdown[sym] - 1);
        }
      }
    },

    /* ================================================================
       Chart.js
       ================================================================ */
    initChart() {
      const canvas = document.getElementById('pnlChart');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');

      Chart.defaults.color = '#8b949e';
      Chart.defaults.borderColor = '#30363d';

      this.chart = new Chart(ctx, {
        type: 'bar',
        data: { labels: [], datasets: [] },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: 'index', intersect: false },
          plugins: {
            legend: {
              labels: { color: '#8b949e', font: { size: 11, family: 'monospace' }, boxWidth: 12, padding: 16 }
            },
            tooltip: {
              backgroundColor: '#1c2333',
              borderColor: '#30363d',
              borderWidth: 1,
              titleColor: '#e6edf3',
              bodyColor: '#8b949e',
              callbacks: {
                label: (ctx) => {
                  const val = ctx.parsed.y;
                  return ` ${ctx.dataset.label}: ${val >= 0 ? '+' : ''}$${val.toFixed(2)}`;
                }
              }
            }
          },
          scales: {
            x: {
              ticks: { color: '#8b949e', font: { size: 10 }, maxTicksLimit: 12 },
              grid: { color: '#21262d' }
            },
            y: {
              ticks: {
                color: '#8b949e', font: { size: 10 },
                callback: (v) => (v >= 0 ? '+' : '') + '$' + v.toFixed(2)
              },
              grid: { color: '#21262d' }
            },
          },
        },
      });

      this.updateChart();
    },

    updateChart() {
      if (!this.chart) return;
      const series = this.metrics.pnl_series || [];

      if (this.chartMode === 'cumulative') {
        this.chart.config.type = 'line';
        this.chart.data.labels = series.map(s => s.date);
        this.chart.data.datasets = [{
          label: 'Cumulative PnL',
          data: series.map(s => s.cumulative_pnl),
          borderColor: '#4488ff',
          backgroundColor: 'rgba(68,136,255,0.08)',
          borderWidth: 2,
          pointRadius: 3,
          pointBackgroundColor: '#4488ff',
          tension: 0.3,
          fill: true,
        }];
      } else if (this.chartMode === 'daily') {
        this.chart.config.type = 'bar';
        this.chart.data.labels = series.map(s => s.date);
        this.chart.data.datasets = [{
          label: 'Daily PnL',
          data: series.map(s => s.daily_pnl),
          backgroundColor: series.map(s => s.daily_pnl >= 0 ? 'rgba(0,255,136,0.6)' : 'rgba(255,68,102,0.6)'),
          borderColor: series.map(s => s.daily_pnl >= 0 ? '#00ff88' : '#ff4466'),
          borderWidth: 1,
          borderRadius: 3,
        }];
      } else if (this.chartMode === 'pertrade') {
        const trades = this.trades.trades || [];
        this.chart.config.type = 'bar';
        this.chart.data.labels = trades.map((t, i) => `#${i + 1} ${t.symbol || ''}`);
        this.chart.data.datasets = [{
          label: 'Trade PnL',
          data: trades.map(t => parseFloat(t.pnl_usd) || 0),
          backgroundColor: trades.map(t => (parseFloat(t.pnl_usd) || 0) >= 0 ? 'rgba(0,255,136,0.6)' : 'rgba(255,68,102,0.6)'),
          borderColor: trades.map(t => (parseFloat(t.pnl_usd) || 0) >= 0 ? '#00ff88' : '#ff4466'),
          borderWidth: 1,
          borderRadius: 3,
        }];
      }

      this.chart.update();
    },

    setChartMode(mode) {
      this.chartMode = mode;
      if (mode === 'pertrade') this.fetchTrades().then(() => this.updateChart());
      else this.updateChart();
    },

    /* ================================================================
       Trade journal pagination & filtering
       ================================================================ */
    nextPage() {
      const maxPage = Math.max(0, Math.ceil(this.trades.total / this.tradePageSize) - 1);
      if (this.tradePage < maxPage) {
        this.tradePage++;
        this.fetchTrades();
      }
    },

    prevPage() {
      if (this.tradePage > 0) {
        this.tradePage--;
        this.fetchTrades();
      }
    },

    applyFilter() {
      this.tradePage = 0;
      this.fetchTrades();
    },

    /* ================================================================
       Control
       ================================================================ */
    async sendControl(action) {
      try {
        const res = await fetch('/api/control', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action }),
        });
        const d = await res.json();
        if (d.ok) await this.fetchStatus();
        return d;
      } catch (e) {
        console.error('[control]', e);
        return { ok: false, message: String(e) };
      }
    },

    async toggleDryRun() {
      await this.sendControl('toggle_dry_run');
    },

    async toggleBot() {
      const action = this.status.bot_running ? 'stop' : 'start';
      await this.sendControl(action);
    },

    /* ================================================================
       CSV Export
       ================================================================ */
    async exportCsv() {
      const { symbol, mode } = this.tradeFilter;
      const params = new URLSearchParams({ limit: 9999, page: 0, symbol: symbol || '' });
      const url = `/api/trades?${params}`;
      try {
        const res = await fetch(url);
        const d = await res.json();
        const trades = d.trades || [];
        if (!trades.length) return;

        const headers = Object.keys(trades[0]);
        const rows = [headers.join(',')];
        for (const t of trades) {
          rows.push(headers.map(h => {
            const v = t[h];
            if (v == null) return '';
            const s = String(v);
            return s.includes(',') ? `"${s}"` : s;
          }).join(','));
        }

        const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = `trades_${new Date().toISOString().slice(0, 10)}.csv`;
        a.click();
      } catch (e) {
        console.error('[csv]', e);
      }
    },

    /* ================================================================
       Template helpers (called from HTML)
       ================================================================ */
    statusPillClass() {
      if (!this.status.bot_running) return 'status-pill stopped';
      if (this.status.dry_run) return 'status-pill dryrun';
      return 'status-pill live';
    },

    statusPillText() {
      if (!this.status.bot_running) return 'STOPPED';
      if (this.status.dry_run) return 'DRY RUN';
      return 'LIVE';
    },

    // Nearest funding countdown (min across all symbols)
    nearestCountdown() {
      const vals = Object.values(this.countdown);
      if (!vals.length) return null;
      return Math.min(...vals);
    },

    nearestCountdownSymbol() {
      let min = Infinity, sym = null;
      for (const [s, v] of Object.entries(this.countdown)) {
        if (v < min) { min = v; sym = s; }
      }
      return sym;
    },

    todayPnlClass() {
      const v = this.metrics.today?.pnl_usd ?? 0;
      return v >= 0 ? 'text-green' : 'text-red';
    },

    winRateDisplay() {
      const wr = this.metrics.overall?.win_rate ?? 0;
      return (wr * 100).toFixed(1) + '%';
    },

    // State machine helpers
    activeSymbols() {
      return this.status.symbols || [];
    },

    currentState(sym) {
      return this.wsStates[sym] || this.status.states?.[sym] || 'FLAT';
    },

    stateItemClass(sym, state) {
      const cur = this.currentState(sym).toUpperCase();
      const s = state.toUpperCase();
      if (cur === s) {
        let cls = 'state-item active';
        if (s === 'ARMED' || s === 'DECISION_LOCKED') cls += ' armed';
        else if (s === 'IN_POSITION') cls += ' in-position';
        else if (s === 'COOLDOWN') cls += ' cooldown';
        return cls;
      }
      return 'state-item inactive';
    },

    stateDisplayName(state) {
      const map = {
        FLAT: 'FLAT',
        WARMUP: 'WARMUP',
        PRE_FUNDING_SCAN: 'SCAN',
        ARMED: 'ARMED',
        DECISION_LOCKED: 'LOCKED',
        ENTERING: 'ENTERING',
        IN_POSITION: 'IN POSITION',
        EXITING: 'EXITING',
        COOLDOWN: 'COOLDOWN',
      };
      return map[state.toUpperCase()] || state;
    },

    // Scanner helpers
    fundingPctClass: fundingClass,
    scoreColorClass(score) { return scoreColor(score, 10); },

    // Signal helpers
    signalEntries() {
      return Object.entries(this.signals.signals || {});
    },

    longScoreBarWidth(sym) {
      const sig = this.signals.signals?.[sym];
      if (!sig) return '0%';
      return Math.min(100, (sig.long_score / 9) * 100) + '%';
    },

    shortScoreBarWidth(sym) {
      const sig = this.signals.signals?.[sym];
      if (!sig) return '0%';
      return Math.min(100, (sig.short_score / 8) * 100) + '%';
    },

    decisionBadgeClass: decisionBadgeClass,
    decisionLabel: decisionLabel,
    symbolGroup: symbolGroup,
    symbolGroupBadge: symbolGroupBadge,
    stateColorClass: stateColorClass,

    // Countdown ring
    ringCircumference: RING_C,
    ringDashOffset(sym) {
      const secs = this.countdown[sym] ?? 0;
      return ringDashOffset(secs);
    },
    ringClass(sym) {
      return ringUrgencyClass(this.countdown[sym] ?? 0);
    },

    // Positions
    sideClass(side) {
      return side === 'BUY' ? 'side-buy' : 'side-sell';
    },
    sideLabel(side) {
      return side === 'BUY' ? 'BUY ↑' : 'SELL ↓';
    },
    pnlClass(val) {
      return (parseFloat(val) || 0) >= 0 ? 'text-green' : 'text-red';
    },

    // Trade journal
    tradeRowClass(trade) {
      const pnl = parseFloat(trade.pnl_usd) || 0;
      if (pnl > 0) return 'trade-win';
      if (pnl < 0) return 'trade-loss';
      return '';
    },

    totalPages() {
      return Math.max(1, Math.ceil(this.trades.total / this.tradePageSize));
    },

    tradeRangeStart() {
      return this.tradePage * this.tradePageSize + 1;
    },

    tradeRangeEnd() {
      return Math.min((this.tradePage + 1) * this.tradePageSize, this.trades.total);
    },

    // Formatters exposed to template
    fmt,

    lastUpdateDisplay() {
      if (!this.lastUpdate) return 'Never';
      return this.lastUpdate.toLocaleTimeString('en-US', { hour12: false });
    },
  };
}
