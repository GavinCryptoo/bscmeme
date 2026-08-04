(() => {
  const directFilePreview = window.location.protocol === 'file:';
  const initialChain = new URLSearchParams(window.location.search).get('chain') === 'bsc' ? 'bsc' : 'solana';
  const state = { chain: initialChain, mode: 'paper', limit: 1000, status: null, health: null, config: null, analytics: null, trendWindow: '24h', filters: { token: '', strategy: 'all', outcome: 'all', status: 'all', window: '24h' }, positionsPage: 1, positionPageSize: 10, positionItems: [], closedPage: 1, closedPageSize: 10, closedItems: [], priceUnit: 'usd' };
  const $ = (selector) => document.querySelector(selector);
  const observationWindowText = () => {
    const fallback = 60;
    const seconds = Number(state.config?.strategy_config?.entry?.observation_delay_sec || fallback);
    return seconds >= 60 && seconds % 60 === 0 ? `${seconds / 60} 分钟` : `${seconds} 秒`;
  };
  document.querySelectorAll('.chain-tab').forEach((item) => {
    const active = item.dataset.chain === state.chain;
    item.classList.toggle('active', active);
    item.setAttribute('aria-selected', active ? 'true' : 'false');
  });
  const escapeHtml = (value) => String(value ?? '').replace(/[&<>'"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[char]));
  const shortMint = (value) => {
    const text = String(value || '—');
    return text.length > 18 ? `${text.slice(0, 8)}…${text.slice(-7)}` : text;
  };
  const dateText = (value) => {
    if (!value) return '—';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '—';
    return date.toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
  };
  const numberText = (value) => Number(value || 0).toLocaleString('zh-CN');
  const countText = (value) => value == null || value === '' ? '—' : numberText(value);
  const marketText = (value) => {
    if (value == null || value === '') return '—';
    const number = Number(value);
    return Number.isFinite(number) ? number.toLocaleString('zh-CN', { maximumSignificantDigits: 6 }) : '—';
  };
  const tokenPriceText = (value) => {
    if (value == null || value === '') return '—';
    const number = Number(value);
    if (!Number.isFinite(number)) return '—';
    const absolute = Math.abs(number);
    const maximumFractionDigits = absolute > 0 && absolute < 1
      ? Math.min(18, Math.max(8, Math.ceil(-Math.log10(absolute)) + 7))
      : 8;
    return number.toLocaleString('zh-CN', {
      useGrouping: false,
      maximumFractionDigits,
    });
  };
  const tradeTimeText = (value, status) => {
    if (status === 'unknown' || value == null || value === '') return '时间口径未知';
    return dateText(value);
  };
  const decimalText = (value, digits) => {
    const number = Number(value);
    return Number.isFinite(number) ? number.toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits }) : '—';
  };
  const signedDecimalText = (value, digits) => {
    if (value == null || value === '') return '—';
    const number = Number(value);
    if (!Number.isFinite(number)) return '—';
    return `${number > 0 ? '+' : ''}${decimalText(value, digits)}`;
  };
  const nativeSymbol = () => state.chain === 'bsc' ? 'BNB' : 'SOL';
  const nativeField = (item, nativeName, legacySolName) => item[nativeName] ?? item[legacySolName];
  const displayTokenName = (item) => [item.symbol, item.display_name, shortMint(item.mint)].filter(Boolean).join(' · ') || '未命名代币';
  const priceSourceText = (snapshot) => ({ jupiter_quote: 'Jupiter 可执行报价', pump_bonding_curve_quote: 'Pump Bonding Curve 报价', bonding_curve_quote: 'bonding_curve_quote', flap_bonding_curve_quote: 'flap_bonding_curve_quote', pancakeswap_quote: 'pancakeswap_quote', binance_indicative_reference: 'binance_indicative_reference', pool_wss: '池内实时价格', binance_indicative: 'Binance 兜底价', timeout_fallback: 'Binance 兜底价' }[snapshot?.price_source] || '历史口径不完整');
  const snapshotPrice = (snapshot, legacyValue) => {
    if (state.chain === 'solana' && state.priceUnit === 'usd') return snapshot?.price_usd ?? null;
    return snapshot?.price_native ?? legacyValue ?? null;
  };
  const snapshotStatusText = (item, snapshot) => snapshot ? priceSourceText(snapshot) : (item.price_snapshot_status === 'legacy_incomplete' ? '历史口径不完整' : '价格不可用');
  const renderChainLabels = () => {
    const isBsc = state.chain === 'bsc';
    const symbol = nativeSymbol();
    $('#metric-pnl-caption').textContent = `已实现 · ${symbol}`;
    const priceUnit = !isBsc && state.priceUnit === 'usd' ? 'USD' : symbol;
    $('#closed-buy-price-label').textContent = `买入价格（${priceUnit}/代币）`;
    $('#closed-sell-price-label').textContent = `卖出价格（${priceUnit}/代币）`;
    $('#price-unit-control').hidden = isBsc;
    $('#closed-pnl-label').textContent = `盈亏额（${symbol}）`;
    $('#chain-eyebrow').textContent = `${isBsc ? 'BSC' : 'Solana'} 主网 · Paper / Shadow`;
    $('#status-quote').textContent = isBsc ? 'BSC 链上只读 Quote' : 'Jupiter Quote GET';
    $('#status-rpc').textContent = isBsc ? 'BSC Pair WSS（可选）' : 'Helius · Alchemy 备用';
  };
  const pnlAmountText = (value) => {
    const text = signedDecimalText(value, 6);
    return text === '—' ? text : `${text} ${nativeSymbol()}`;
  };
  const pnlRateText = (value) => {
    const text = signedDecimalText(value, 2);
    return text === '—' ? text : `${text}%`;
  };
  const pnlClass = (value) => Number(value) > 0 ? 'pnl-positive' : 'pnl-negative';
  const copyControl = (mint) => {
    const value = String(mint || '').trim();
    return value ? `<button class="copy-button" type="button" data-copy="${escapeHtml(value)}" title="复制合约地址" aria-label="复制合约地址">复制</button>` : '';
  };
  const modeData = () => state.status?.modes?.[state.mode] || { counts: {} };
  const strategyLabels = {
    sol_ultra_early_baseline: 'Solana 超早期基线策略',
    ultra_early_minimal: '超早期最小规则',
    binance_web3: 'Binance Web3',
    bsc_binance_indicative: 'BSC 链上只读报价 Paper/Shadow 策略',
  };
  const healthLabels = {
    HEALTHY: '健康',
    UNAVAILABLE: '不可用',
    DEGRADED: '部分异常',
    UNKNOWN: '未知',
  };
  const componentLabels = {
    binance_web3: 'Binance Web3',
    coordinator: '协调器',
    jupiter: 'Jupiter Quote',
    solana_rpc: 'Solana RPC',
    solana_wss: 'Solana WSS',
    telegram: '通知控制',
  };
  const statusLabels = {
    ACCEPTED: '通过',
    REJECTED: '拒绝',
    OPEN: '持仓中',
    ENTRY_PENDING: '入场处理中',
    EXIT_TRIGGERED: '退出处理中',
    CLOSED: '已平仓',
  };
  const reasonLabels = {
    token_age_unavailable: '币龄不可用',
    unique_buyers_unavailable: '独立买家数不可用',
    buy_sell_ratio_unavailable: '买卖比不可用',
    net_buy_unavailable: '净买入不可用',
    flow_window_unavailable: '资金流窗口不可用',
    creator_sell_unavailable: '创建者卖出状态不可用',
    buy_price_impact_unavailable: '买入价格影响不可用',
    sell_price_impact_unavailable: '卖出价格影响不可用',
    buy_quote_unavailable: '买入 Quote 不可用',
    sell_quote_unavailable: '卖出 Quote 不可用',
    buy_quote_output_unavailable: '买入 Quote 输出为零',
    sell_quote_output_unavailable: '卖出 Quote 输出为零',
    no_route: '没有有效路线',
    no_liquidity: '没有可用流动性',
    quote_expired: 'Quote 已过期',
    buy_quote_mint_mismatch: '买入 Quote 的 Mint 不匹配',
    sell_quote_mint_mismatch: '卖出 Quote 的 Mint 不匹配',
    sell_quote_quantity_mismatch: '卖出 Quote 数量不匹配',
    buy_sell_ratio_below_min: '买卖比低于要求',
    unique_buyers_below_min: '独立买家数低于要求',
    net_buy_not_positive: '净买入不为正',
    flow_window_negative: '资金流窗口为负',
    creator_confirmed_sold: '已确认创建者卖出',
    token_age_out_of_range: '币龄超出范围',
    max_open_positions_reached: '已达到最大持仓数',
    mint_lifecycle_exists: '该 Mint 已存在生命周期',
    same_name_cooldown: '同名代币仍在冷却期',
    daily_full_loss_limit: '已达到每日亏损上限',
    large_loss_pause: '大亏次数已触发暂停',
    market_cap_unavailable: '入场时市值不可用',
    market_cap_below_min: '入场时市值低于最低值',
    liquidity_unavailable: '入场时流动性不可用',
    liquidity_below_min: '入场时流动性低于最低值',
    holders_unavailable: '入场时持币地址数不可用',
    holders_below_min: '入场时持币地址数低于最低值',
    observation_price_unavailable: '观察期后的价格不可用',
    price_not_up_after_observation: '观察期后价格未高于首次发现价格',
    price_below_after_observation: '观察期后价格低于首次发现价格',
    observation_liquidity_unavailable: '观察期后的流动性不可用',
    observation_liquidity_below_min: '观察期后的流动性低于最低值',
    liquidity_below_first_discovery_after_observation: '观察期后流动性低于首次发现值',
    holders_observation_unavailable: '观察期后的持币地址数不可用',
    holders_below_first_discovery_after_observation: '观察期后持币地址数低于首次发现值',
    shadow_holders_drop_over_10pct: 'Shadow 持币地址数较入场下降超过 10%',
    shadow_liquidity_drop_over_15pct: 'Shadow 流动性较入场下降超过 15%',
    stop_loss: '触发止损',
    take_profit: '达到止盈',
    max_hold_timeout: '超过最长持仓时间',
    holders_drop_early_exit: '持币地址数下降提前退出',
    unknown: '其他',
    runtime_paused: '运行控制已暂停入场',
    jupiter_rate_limited: 'Jupiter 请求受限',
  };
  const strategyLabel = (value) => strategyLabels[value] || '当前策略';
  const healthLabel = (value) => healthLabels[value] || '未知';
  const componentLabel = (value) => componentLabels[value] || '系统组件';
  const statusLabel = (value) => statusLabels[value] || '处理中';
  const reasonText = (value) => {
    if (value === 'other') return '其他';
    const observation = observationWindowText();
    const labels = {
      observation_price_unavailable: `${observation}观察后的价格不可用`,
      price_not_up_after_observation: `${observation}后价格未高于首次发现价格`,
      price_below_after_observation: `${observation}后价格低于首次发现价格`,
      observation_liquidity_unavailable: `${observation}观察后的流动性不可用`,
      observation_liquidity_below_min: `${observation}观察后的流动性低于最低值`,
      liquidity_below_first_discovery_after_observation: `${observation}后流动性低于首次发现值`,
      holders_observation_unavailable: `${observation}观察后的持币地址数不可用`,
      holders_below_first_discovery_after_observation: `${observation}后持币地址数低于首次发现值`,
    };
    return String(value || '').split(',').map((item) => labels[item] || reasonLabels[item] || '其他过滤条件').join('、');
  };
  const errorLabel = (value) => reasonLabels[value] || ({ binance_rate_limited: 'Binance 请求受限', binance_timeout: 'Binance 请求超时', jupiter_http_error: 'Jupiter 接口错误', jupiter_connection_error: 'Jupiter 连接错误' }[value] || '无活动错误');
  const showToast = (message) => {
    const toast = $('#toast');
    toast.textContent = message;
    toast.classList.add('show');
    window.clearTimeout(showToast.timer);
    showToast.timer = window.setTimeout(() => toast.classList.remove('show'), 2200);
  };

  async function getJson(path) {
    const separator = path.includes('?') ? '&' : '?';
    const requestPath = `${path}${separator}chain=${encodeURIComponent(state.chain)}`;
    const response = await fetch(requestPath, { cache: 'no-store' });
    if (!response.ok) throw new Error(`${path} ${response.status}`);
    return response.json();
  }

  async function load() {
    if (directFilePreview) {
      $('#file-protocol-notice').hidden = false;
      $('#connection-pill').className = 'connection-pill bad';
      $('#connection-pill span').textContent = '请使用服务地址';
      $('#runtime-state').textContent = '本地文件预览';
      return;
    }
    try {
      const mode = state.mode;
      const limit = state.limit;
      const analyticsQuery = new URLSearchParams({ mode, strategy: state.filters.strategy, token: state.filters.token, outcome: state.filters.outcome, window: state.filters.window, trend_window: state.trendWindow });
      const [status, health, config, positions, closedPositions, analytics] = await Promise.all([
        getJson('/api/status'),
        getJson('/api/health'),
        getJson('/api/config'),
        getJson(`/api/positions?mode=${mode}&status=OPEN&limit=${limit}`),
        getJson(`/api/positions?mode=${mode}&status=CLOSED&limit=${limit}`),
        getJson(`/api/analytics?${analyticsQuery.toString()}`),
      ]);
      state.status = status;
      state.health = health;
      state.config = config;
      state.analytics = analytics;
      render(status, health, positions.items || [], closedPositions.items || [], analytics);
    } catch (error) {
      $('#connection-pill').className = 'connection-pill bad';
      $('#connection-pill span').textContent = '连接异常';
      $('#runtime-state').textContent = '数据不可用';
      $('#health-summary').textContent = '接口错误';
    }
  }

  function render(status, health, positions, closedPositions, analytics) {
    renderChainLabels();
    const data = modeData();
    const counts = data.counts || {};
    const summary = analytics?.summary || {};
    const healthy = status.status === 'ok';
    $('#connection-pill').className = `connection-pill ${healthy ? 'ok' : 'bad'}`;
    $('#connection-pill span').textContent = healthy ? '在线 · 只读' : '状态异常';
    $('#runtime-state').textContent = healthy ? '运行正常' : '需要检查';
    $('#last-updated').textContent = `更新 ${dateText(new Date().toISOString())}`;
    const pnl = data.pnl || {};
    $('#metric-signals').textContent = numberText(summary.candidate_count ?? counts.signals);
    const profitableTokens = Number(summary.profitable_trade_count ?? pnl.profitable_trade_count ?? 0);
    const losingTokens = Number(summary.losing_trade_count ?? pnl.losing_trade_count ?? 0);
    $('#metric-pnl-tokens').innerHTML = `<span class="pnl-positive">${numberText(profitableTokens)}</span> / <span class="pnl-negative">${numberText(losingTokens)}</span>`;
    $('#metric-open').textContent = numberText(summary.open_positions ?? counts.open_positions);
    $('#metric-closed').textContent = numberText(summary.closed_positions ?? counts.closed_positions);
    $('#metric-pnl-rate').textContent = `${decimalText(summary.rate_pct ?? pnl.rate_pct, 2)}%`;
    $('#metric-pnl-amount').textContent = `${decimalText(summary.amount_native ?? pnl.amount_native ?? pnl.amount_sol, 6)} ${nativeSymbol()}`;
    $('#position-mode-tag').textContent = state.mode === 'paper' ? 'Paper' : 'Shadow';
    $('#position-mode-tag').className = `tag ${state.mode === 'paper' ? 'blue' : 'neutral'}`;
    renderHealth(health);
    renderPositions(filterPositionRows(positions, 'OPEN'));
    renderClosedPositions(filterPositionRows(closedPositions, 'CLOSED'));
    renderConfig(state.config);
    renderAnalytics(analytics);
  }

  const chartColors = ['#b9f36f', '#63b7ff', '#ffc266', '#bb9cff', '#ff8d97', '#7ee0c5', '#f7a1ff', '#8da8ff'];
  const chartColor = (index) => chartColors[index % chartColors.length];
  const canvasContext = (canvas) => {
    if (!canvas) return null;
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(1, Math.round(rect.width || canvas.width));
    const height = Math.max(1, Math.round(rect.height || canvas.height));
    const ratio = window.devicePixelRatio || 1;
    canvas.width = width * ratio;
    canvas.height = height * ratio;
    const context = canvas.getContext('2d');
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, width, height);
    return { context, width, height };
  };
  const drawDonut = (canvas, items) => {
    const surface = canvasContext(canvas);
    if (!surface) return;
    const { context, width, height } = surface;
    const total = items.reduce((sum, item) => sum + Number(item.count || 0), 0);
    const radius = Math.min(width, height) * 0.34;
    const centerX = width * 0.5;
    const centerY = height * 0.5;
    context.lineWidth = Math.max(14, Math.min(24, width * 0.095));
    if (!total) {
      context.beginPath();
      context.strokeStyle = 'rgba(163,202,220,.12)';
      context.arc(centerX, centerY, radius, 0, Math.PI * 2);
      context.stroke();
    } else {
      let start = -Math.PI / 2;
      items.forEach((item, index) => {
        const amount = Number(item.count || 0);
        const end = start + (amount / total) * Math.PI * 2;
        context.beginPath();
        context.strokeStyle = chartColor(index);
        context.arc(centerX, centerY, radius, start + 0.018, end - 0.018);
        context.stroke();
        start = end;
      });
    }
    context.textAlign = 'center';
    context.textBaseline = 'middle';
    context.fillStyle = '#e7f1f4';
    context.font = '700 25px Inter, system-ui, sans-serif';
    context.fillText(numberText(total), centerX, centerY - 7);
    context.fillStyle = '#87a2ae';
    context.font = '10px Inter, system-ui, sans-serif';
    context.fillText('记录', centerX, centerY + 17);
  };
  const renderChartLegend = (target, items, emptyText) => {
    if (!target) return;
    const total = items.reduce((sum, item) => sum + Number(item.count || 0), 0);
    target.innerHTML = items.length ? items.map((item, index) => `<div class="chart-legend-row"><i style="--legend-color:${chartColor(index)}"></i><span class="chart-legend-name">${escapeHtml(reasonText(item.reason))}</span><strong>${numberText(item.count)}</strong><em>${decimalText(item.share_pct, 1)}%</em></div>`).join('') : `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
    return total;
  };
  const drawTrend = (canvas, points) => {
    const surface = canvasContext(canvas);
    if (!surface) return;
    const { context, width, height } = surface;
    const padding = { left: 46, right: 18, top: 18, bottom: 30 };
    const plotWidth = width - padding.left - padding.right;
    const plotHeight = height - padding.top - padding.bottom;
    const amounts = points.map((item) => Number(item.amount_native || 0));
    const cumulative = points.map((item) => Number(item.cumulative_native || 0));
    const values = [...amounts, ...cumulative, 0];
    let min = Math.min(...values);
    let max = Math.max(...values);
    if (!Number.isFinite(min) || !Number.isFinite(max)) { min = -1; max = 1; }
    if (min === max) { min -= 1; max += 1; }
    const pad = (max - min) * 0.12;
    min -= pad; max += pad;
    const x = (index) => padding.left + (points.length <= 1 ? plotWidth / 2 : (index / (points.length - 1)) * plotWidth);
    const y = (value) => padding.top + ((max - value) / (max - min)) * plotHeight;
    context.font = '10px Inter, system-ui, sans-serif';
    context.textAlign = 'right';
    context.textBaseline = 'middle';
    for (let step = 0; step < 5; step += 1) {
      const value = min + ((max - min) * step) / 4;
      const lineY = y(value);
      context.beginPath();
      context.strokeStyle = 'rgba(163,202,220,.11)';
      context.lineWidth = 1;
      context.moveTo(padding.left, lineY);
      context.lineTo(width - padding.right, lineY);
      context.stroke();
      context.fillStyle = '#58717c';
      context.fillText(decimalText(value, 4), padding.left - 9, lineY);
    }
    const zeroY = y(0);
    context.beginPath();
    context.strokeStyle = 'rgba(231,241,244,.22)';
    context.setLineDash([4, 4]);
    context.moveTo(padding.left, zeroY);
    context.lineTo(width - padding.right, zeroY);
    context.stroke();
    context.setLineDash([]);
    const drawLine = (valuesToDraw, color, widthValue) => {
      if (!valuesToDraw.length) return;
      context.beginPath();
      valuesToDraw.forEach((value, index) => index ? context.lineTo(x(index), y(value)) : context.moveTo(x(index), y(value)));
      context.strokeStyle = color;
      context.lineWidth = widthValue;
      context.lineJoin = 'round';
      context.lineCap = 'round';
      context.stroke();
      context.fillStyle = color;
      valuesToDraw.forEach((value, index) => { if (index % 6 === 0 || index === valuesToDraw.length - 1) { context.beginPath(); context.arc(x(index), y(value), 2.7, 0, Math.PI * 2); context.fill(); } });
    };
    drawLine(cumulative, '#63b7ff', 1.5);
    drawLine(amounts, '#b9f36f', 2.2);
    context.textAlign = 'center';
    context.textBaseline = 'top';
    [0, 6, 12, 18, 23].forEach((index) => {
      if (!points[index]) return;
      const timestamp = new Date(points[index].timestamp);
      context.fillStyle = '#58717c';
      context.fillText(timestamp.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', hour12: false }), x(index), height - padding.bottom + 10);
    });
  };
  const renderAnalytics = (analytics) => {
    if (!analytics) return;
    const strategySelect = $('#filter-strategy');
    const strategyOptions = analytics.strategies || [];
    const currentStrategy = state.filters.strategy;
    strategySelect.innerHTML = `<option value="all">全部策略</option>${strategyOptions.map((item) => `<option value="${escapeHtml(item.strategy_name)}">${escapeHtml(strategyLabel(item.strategy_name))}</option>`).join('')}`;
    state.filters.strategy = strategyOptions.some((item) => item.strategy_name === currentStrategy) ? currentStrategy : 'all';
    strategySelect.value = state.filters.strategy;
    const lossItems = analytics.loss_reasons || [];
    const rejectionItems = analytics.rejection_reasons || [];
    const unavailableItems = analytics.unavailable_reasons || [];
    drawDonut($('#loss-reason-chart'), lossItems);
    drawDonut($('#rejection-reason-chart'), rejectionItems);
    drawDonut($('#unavailable-reason-chart'), unavailableItems);
    const lossTotal = renderChartLegend($('#loss-reason-legend'), lossItems, '暂无亏损记录');
    const rejectionTotal = renderChartLegend($('#rejection-reason-legend'), rejectionItems, '暂无拒绝记录');
    const unavailableTotal = renderChartLegend($('#unavailable-reason-legend'), unavailableItems, '暂无不可用字段');
    $('#loss-reason-total').textContent = `${numberText(lossTotal || 0)} 笔`;
    $('#rejection-reason-total').textContent = `${numberText(rejectionTotal || 0)} 次原因`;
    $('#unavailable-reason-total').textContent = `${numberText(unavailableTotal || 0)} 次标记`;
    const trend = analytics.pnl_trend || [];
    drawTrend($('#pnl-trend-chart'), trend);
    $('#trend-unit').textContent = `${analytics.native_symbol || nativeSymbol()} · ${analytics.mode === 'paper' ? 'Paper' : 'Shadow'}`;
    state.trendWindow = analytics.trend_window || state.trendWindow;
    $('#trend-window').value = state.trendWindow;
    const strategyTarget = $('#strategy-results');
    strategyTarget.innerHTML = strategyOptions.length ? strategyOptions.map((item) => {
      const itemPnl = item.pnl || {};
      return `<article class="strategy-result-card"><div class="strategy-result-head"><div><span class="strategy-chip">${escapeHtml(item.strategy_name)}</span><h3>${escapeHtml(strategyLabel(item.strategy_name))}</h3></div><span class="strategy-result-pnl ${pnlClass(itemPnl.amount_native)}">${escapeHtml(pnlAmountText(itemPnl.amount_native))}</span></div><div class="strategy-result-stats"><div><span>盈亏率</span><strong class="${pnlClass(itemPnl.rate_pct)}">${escapeHtml(pnlRateText(itemPnl.rate_pct))}</strong></div><div><span>盈利 / 亏损</span><strong>${numberText(itemPnl.profitable_trade_count)} / ${numberText(itemPnl.losing_trade_count)}</strong></div><div><span>已平仓</span><strong>${numberText(item.closed_positions)}</strong></div><div><span>拒绝</span><strong>${numberText(item.rejected_count)}</strong></div></div></article>`;
    }).join('') : '<div class="empty-state">当前筛选没有策略记录</div>';
    $('#strategy-results-summary').textContent = `${numberText(analytics.summary?.closed_trade_count || 0)} 笔已实现交易 · 各策略独立计算`;
  };
  const filterPositionRows = (rows, expectedStatus) => rows.filter((item) => {
    const filter = state.filters;
    if (filter.status !== 'all' && filter.status !== expectedStatus) return false;
    if (filter.strategy !== 'all' && item.strategy_name !== filter.strategy) return false;
    const token = filter.token.trim().toLowerCase();
    if (token && ![item.token_name, item.mint].some((value) => String(value || '').toLowerCase().includes(token))) return false;
    if (filter.outcome !== 'all') {
      const pnl = Number(nativeField(item, 'pnl_bnb', 'pnl_sol'));
      if (expectedStatus !== 'CLOSED' || (filter.outcome === 'profit' && pnl <= 0) || (filter.outcome === 'loss' && pnl >= 0)) return false;
    }
    if (filter.window !== 'all') {
      const dateValue = item.closed_at || item.opened_at;
      const date = dateValue ? new Date(dateValue) : null;
      const cutoff = Date.now() - (filter.window === '7d' ? 7 : 1) * 24 * 60 * 60 * 1000;
      if (!date || Number.isNaN(date.getTime()) || date.getTime() < cutoff) return false;
    }
    return true;
  });

  function renderHealth(health) {
    const modeHealth = health?.modes?.[state.mode] || {};
    const items = modeHealth.snapshot?.items || {};
    const rows = Object.values(items);
    const hasBad = rows.some((item) => item.state && item.state !== 'HEALTHY');
    $('#health-summary').textContent = hasBad ? '部分异常' : '全部正常';
    $('#health-summary').className = `tag ${hasBad ? '' : 'blue'}`;
    $('#health-list').innerHTML = rows.length ? rows.map((item) => `
      <div class="health-item"><div class="health-item-top"><span class="health-name">${escapeHtml(componentLabel(item.name))}</span><span class="health-state ${item.state !== 'HEALTHY' ? 'bad' : ''}">${escapeHtml(healthLabel(item.state))}</span></div><div class="health-meta"><span>${escapeHtml(errorLabel(item.error_class))}</span><span>${item.latency_ms == null ? '—' : `${item.latency_ms} 毫秒`}</span></div></div>
    `).join('') : '<div class="empty-state">暂无健康数据</div>';
  }

  function renderPositions(positions) {
    state.positionItems = positions;
    const totalPages = Math.max(1, Math.ceil(positions.length / state.positionPageSize));
    state.positionsPage = Math.min(state.positionsPage, totalPages);
    const start = (state.positionsPage - 1) * state.positionPageSize;
    const visible = positions.slice(start, start + state.positionPageSize);
    $('#position-page').textContent = positions.length ? `第 ${state.positionsPage} / ${totalPages} 页` : '暂无持仓';
    $('#position-prev').disabled = state.positionsPage <= 1;
    $('#position-next').disabled = state.positionsPage >= totalPages;
    const isBsc = state.chain === 'bsc';
    $('#position-body').innerHTML = visible.length ? visible.map((item) => {
      const source = isBsc ? bscPriceSourceLabel(item.price_source) : 'pool_wss_indicative';
      const executable = isBsc ? source : 'jupiter_quote';
      return `<tr><td><span class="mint">${escapeHtml(displayTokenName(item))}</span>${copyControl(item.mint)}</td><td><strong>${escapeHtml(tokenPriceText(item.local_price_sol_per_token))}</strong><small class="table-subtext">${escapeHtml(source)}</small></td><td><strong>${escapeHtml(tokenPriceText(item.jupiter_price_sol_per_token))}</strong><small class="table-subtext">${escapeHtml(executable)}</small></td><td>${escapeHtml(signedDecimalText(item.price_delta_pct, 2))}%</td><td>${escapeHtml(dateText(item.local_price_observed_at))}</td><td>${escapeHtml(dateText(item.jupiter_price_observed_at))}</td><td>${escapeHtml(countText(item.holders))}</td><td>${escapeHtml(marketText(item.market_cap_usd))}</td><td>${escapeHtml(marketText(item.liquidity_usd))}</td></tr>`;
    }).join('') : '<tr><td colspan="9" class="empty-state">暂无虚拟持仓</td></tr>';
  }

  function bscPriceSourceLabel(source) {
    if (source === 'flap_bonding_curve_quote') return 'flap_bonding_curve_quote';
    if (source === 'bonding_curve' || source === 'bonding_curve_quote') return 'bonding_curve_quote';
    if (source === 'pancakeswap_router' || source === 'pancakeswap_quote') return 'pancakeswap_quote';
    return 'binance_indicative_reference';
  }

  function renderClosedPositions(positions) {
    state.closedItems = positions;
    const pageSizeSelect = $('#closed-page-size');
    if (pageSizeSelect) pageSizeSelect.value = String(state.closedPageSize);
    const totalPages = Math.max(1, Math.ceil(positions.length / state.closedPageSize));
    state.closedPage = Math.min(state.closedPage, totalPages);
    const start = (state.closedPage - 1) * state.closedPageSize;
    const visible = positions.slice(start, start + state.closedPageSize);
    $('#closed-count').textContent = `${positions.length} 条记录`;
    $('#closed-page').textContent = positions.length ? `第 ${state.closedPage} / ${totalPages} 页` : '暂无记录';
    $('#closed-prev').disabled = state.closedPage <= 1;
    $('#closed-next').disabled = state.closedPage >= totalPages;
    $('#closed-body').innerHTML = visible.length
      ? visible.map((item) => {
        const legacyBuy = nativeField(item, 'buy_price_bnb', 'buy_price_sol');
        const legacySell = nativeField(item, 'sell_price_bnb', 'sell_price_sol');
        const buyPrice = snapshotPrice(item.entry_price_snapshot, legacyBuy);
        const sellPrice = snapshotPrice(item.exit_price_snapshot, legacySell);
        const buySource = snapshotStatusText(item, item.entry_price_snapshot);
        const sellSource = snapshotStatusText(item, item.exit_price_snapshot);
        const pnl = nativeField(item, 'pnl_bnb', 'pnl_sol');
        return `<tr><td><span class="mint">${escapeHtml(displayTokenName(item))}</span>${copyControl(item.mint)}</td><td>${escapeHtml(tradeTimeText(item.signal_observed_at, item.signal_observed_at ? 'known' : 'unknown'))}</td><td>${escapeHtml(tradeTimeText(item.evaluated_at, item.evaluated_at ? 'known' : 'unknown'))}</td><td>${escapeHtml(tradeTimeText(item.entry_quote_at, item.entry_time_status))}</td><td>${escapeHtml(tradeTimeText(item.opened_at, item.opened_at ? 'known' : 'unknown'))}</td><td>${escapeHtml(tradeTimeText(item.exit_quote_at, item.exit_time_status))}</td><td>${escapeHtml(tradeTimeText(item.closed_at, item.closed_at ? 'known' : 'unknown'))}</td><td><strong>${escapeHtml(tokenPriceText(buyPrice))}</strong><small class="table-subtext">${escapeHtml(buySource)}</small></td><td><strong>${escapeHtml(tokenPriceText(sellPrice))}</strong><small class="table-subtext">${escapeHtml(sellSource)}</small></td><td><span class="${pnlClass(pnl)}">${escapeHtml(pnlAmountText(pnl))}</span></td><td><span class="${pnlClass(item.pnl_rate_pct)}">${escapeHtml(pnlRateText(item.pnl_rate_pct))}</span></td><td>${escapeHtml(countText(item.entry_holders ?? item.holders))}</td><td>${escapeHtml(exitHoldersText(item))}</td><td>${escapeHtml(marketText(item.entry_market_cap_usd))}</td><td>${escapeHtml(marketText(item.exit_market_cap_usd))}</td><td>${escapeHtml(marketText(item.entry_liquidity_usd))}</td><td>${escapeHtml(marketText(item.exit_liquidity_usd))}</td></tr>`;
      }).join('')
      : '<tr><td colspan="17" class="empty-state">暂无已平仓记录</td></tr>';
  }

  function exitHoldersText(item) {
    if (item.exit_holders_status === 'pending') return '补录中';
    if (item.exit_holders_status === 'unavailable') return '—';
    return countText(item.exit_holders);
  }

  function renderConfig(config) {
    if (!config) return;
    const isBsc = config.chain_key === 'bsc';
    $('#strategy-icon').textContent = isBsc ? 'B' : 'S';
    $('#strategy-name').textContent = config.display_name || strategyLabel(config.strategy || (isBsc ? 'bsc_binance_indicative' : 'sol_ultra_early_baseline'));
    $('#strategy-version').textContent = `${config.display_ruleset_name || strategyLabel(config.ruleset_name || 'ultra_early_minimal')} · v${config.ruleset_version || '0.1.0'}`;
    const entry = config.strategy_config?.entry || {};
    const paper = config.strategy_config?.paper_exit || {};
    const age = entry.token_age_sec || [5, 120];
    const observation = observationWindowText();
    $('#strategy-summary').innerHTML = isBsc
      ? `<div class="summary-line"><span>定价模式</span><strong>BSC 链上可执行只读报价</strong></div><div class="summary-line"><span>执行报价</span><strong>可执行只读 Quote · 不广播</strong></div><div class="summary-line"><span>参考价格</span><strong>Binance 仅作 Dashboard 对照，不参与 PnL</strong></div><div class="summary-line"><span>观察期</span><strong>${escapeHtml(observation)}后价格必须上涨，持币地址数不得低于首次发现</strong></div><div class="summary-line"><span>Paper / Shadow退出</span><strong>止盈 +${escapeHtml(paper.take_profit_pct || '10')}% / 止损 ${escapeHtml(paper.stop_loss_trigger_pct || '-10')}% / 超时 ${escapeHtml(paper.max_hold_sec || 600)} 秒</strong></div><div class="summary-line"><span>虚拟仓位</span><strong>${escapeHtml(config.strategy_config?.risk?.position_size_bnb || '0.001')} BNB</strong></div>`
      : `<div class="summary-line"><span>入场窗口</span><strong>${escapeHtml(age[0])}–${escapeHtml(age[1])} 秒</strong></div><div class="summary-line"><span>观察期</span><strong>${escapeHtml(observation)}；价格、持币地址、市值、流动性仅记录</strong></div><div class="summary-line"><span>买家门槛</span><strong>≥ ${escapeHtml(entry.unique_buyers_15s_min || 6)} / 15 秒</strong></div><div class="summary-line"><span>止盈 / 止损</span><strong>+${escapeHtml(paper.take_profit_pct || '10')}% / ${escapeHtml(paper.stop_loss_trigger_pct || '-20')}%</strong></div><div class="summary-line"><span>最长持仓</span><strong>${escapeHtml(paper.max_hold_sec || 600)} 秒</strong></div>`;
  }

  function openModal() {
    $('#strategy-modal').classList.remove('hidden');
    renderStrategyModal();
  }

  function closeModal() { $('#strategy-modal').classList.add('hidden'); }

  function renderStrategyModal() {
    const config = state.config || {};
    const groups = config.strategy_config || {};
    const isBsc = config.chain_key === 'bsc';
    const observation = observationWindowText();
    $('#strategy-modal-subtitle').textContent = isBsc
      ? 'BSC 当前运行配置：链上只读报价 Paper/Shadow；Binance 仅作参考，不包含钱包、签名、广播或真实交易。'
      : 'Solana 当前运行基线：Jupiter Quote 只读 Paper/Shadow，不包含真实交易。';
    const labels = { entry: isBsc ? 'BSC 入场与定价规则' : '入场硬条件', paper_exit: 'Paper 退出规则', shadow_exit: 'Shadow 规则', risk: '虚拟风控' };
    const textFor = (key, value) => {
      const names = {
        token_age_sec: `币龄 ${value[0]}–${value[1]} 秒`, unique_buyers_15s_min: `15 秒独立买家数 ≥ ${value}`, buy_sell_count_ratio_15s_min: `15 秒买卖笔数比 ≥ ${value}`, net_buy_15s: `15 秒净买入 > 0`, require_two_non_negative_flow_windows: '两个短窗口净流量均非负', creator_confirmed_sold_at_entry: '创建者确认卖出必须为否', require_executable_buy_route: '必须存在可执行买入 Quote', require_executable_sell_route: '必须存在可执行卖出 Quote', max_buy_price_impact_pct: `买入价格影响 ≤ ${value}%`, max_immediate_exit_impact_pct: `即时卖出价格影响 ≤ ${value}%`, min_holders: `${groups.entry?.holders_policy === 'record_only' ? '持币地址数仅记录' : `${groups.entry?.min_holders_inclusive ? '入场时持币地址数 ≥' : '入场时持币地址数 >'} ${value}`}`, min_market_cap_usd: groups.entry?.market_cap_policy === 'record_only' ? '市值仅记录' : `入场时市值 ≥ ${value} USD`, min_liquidity_usd: groups.entry?.liquidity_policy === 'record_only' ? '流动性仅记录' : `入场时流动性 ≥ ${value} USD`, take_profit_pct: `止盈 ≥ ${value}% · 全部退出`, stop_loss_trigger_pct: `止损 ≤ ${value}% · 全部退出`, max_hold_sec: `最长持仓 ${value} 秒`, take_profit_sell_pct: `止盈卖出 ${value}%`, stop_loss_sell_pct: `止损卖出 ${value}%`, partial_take_profit_enabled: '不启用分批止盈', moving_stop_enabled: '不启用移动止损', shadow_holders_drop_pct: `Shadow 持币地址数下降超过 ${value}% 时提前退出`, shadow_liquidity_drop_pct: `Shadow 流动性下降超过 ${value}% 时提前退出`, shadow_defense_pct: `防御退出收益率 ≤ ${value}%`, shadow_time_sec: `Shadow 时间退出 ≥ ${value} 秒`, shadow_mfe_pct: `Shadow MFE 门槛 ${value}%`, rules: 'Shadow 规则组', position_size_sol: `单笔虚拟仓位 ${value} SOL`, initial_virtual_balance_sol: `初始虚拟余额 ${value} SOL`, max_open_positions: `最大同时持仓 ${value}`, same_name_cooldown_sec: `同名冷却 ${value} 秒`, one_trade_per_mint: '同一 Mint 只允许一个生命周期', daily_full_loss_sol_limit: `每日完整亏损上限 ${value} SOL`, pause_new_entries_after_large_losses: `大亏 ${value} 次后暂停新入场`, large_loss_threshold_pct: `大亏阈值 ${value}%`,
        pricing_mode: '定价模式：BSC 链上只读报价', executable_quote: value ? '可执行只读报价：是（不广播）' : '可执行报价：否', require_readonly_buy_quote: '必须取得非零链上买入 Quote', require_readonly_sell_quote: '必须取得非零链上即时卖出 Quote', binance_indicative_reference_only: 'Binance 指示价仅作 Dashboard 对照，不参与 PnL', observation_delay_sec: `观察期 ${observation}`, observation_price_rise_required: value ? `${observation}观察后价格必须高于首次发现价格` : `${observation}观察后价格仅记录`, observation_price_policy: value === 'hard' ? `${observation}观察后价格为硬条件` : `${observation}观察后价格仅记录，不阻断入场`, observation_liquidity_policy: value === 'hard' ? `${observation}观察后流动性为硬条件` : `${observation}观察后流动性仅记录，不阻断入场`, holders_policy: value === 'hard' ? '持币地址数为硬条件' : '持币地址数仅记录，不阻断入场', market_cap_policy: value === 'hard' ? '市值为硬条件' : '市值仅记录，不阻断入场', liquidity_policy: value === 'hard' ? '流动性为硬条件' : '流动性仅记录，不阻断入场', require_holders_non_decreasing_after_observation: value ? `${observation}观察后持币地址数不得低于首次发现值` : '观察后持币地址数仅记录，不设下降门槛', optional_unavailable_fields: `不可用字段（仅记录、不阻断）：${Array.isArray(value) ? value.join('、') : value}`, position_size_bnb: `单笔虚拟仓位 ${value} BNB`, initial_virtual_balance_bnb: `初始虚拟余额 ${value} BNB`, daily_full_loss_bnb: `每日完整亏损上限 ${value} BNB`,
      };
      if (key === 'rules') {
        const ruleLabels = {
          paper_take_profit: `按 Paper 止盈 ≥ ${groups.paper_exit?.take_profit_pct || '10'}% · 全部退出`,
          paper_stop_loss: `按 Paper 止损 ≤ ${groups.paper_exit?.stop_loss_trigger_pct || '-10'}% · 全部退出`,
          paper_max_hold_timeout: `按 Paper 最长持仓 ${groups.paper_exit?.max_hold_sec || 600} 秒退出`,
        };
        return `规则：${Array.isArray(value) ? value.map((rule) => ruleLabels[rule] || rule).join(' / ') : value}`;
      }
      return names[key] || `${key}: ${Array.isArray(value) ? value.join(' / ') : value}`;
    };
    $('#strategy-modal-content').innerHTML = Object.entries(groups).map(([group, values]) => `<section class="rule-group"><h3>${escapeHtml(labels[group] || group)}</h3><ul class="rule-list">${Object.entries(values).map(([key, value]) => `<li>${escapeHtml(textFor(key, value))}</li>`).join('')}</ul></section>`).join('') || '<div class="empty-state">暂无策略配置</div>';
  }

  document.addEventListener('click', async (event) => {
    if (event.target.closest('#position-prev')) {
      state.positionsPage = Math.max(1, state.positionsPage - 1);
      renderPositions(state.positionItems);
      return;
    }
    if (event.target.closest('#position-next')) {
      state.positionsPage += 1;
      renderPositions(state.positionItems);
      return;
    }
    if (event.target.closest('#closed-prev')) {
      state.closedPage = Math.max(1, state.closedPage - 1);
      renderClosedPositions(state.closedItems);
      return;
    }
    if (event.target.closest('#closed-next')) {
      state.closedPage += 1;
      renderClosedPositions(state.closedItems);
      return;
    }
    const chainButton = event.target.closest('[data-chain]');
    if (chainButton) {
      state.chain = chainButton.dataset.chain;
      state.filters.strategy = 'all';
      state.positionsPage = 1;
      state.closedPage = 1;
      document.querySelectorAll('.chain-tab').forEach((item) => {
        const active = item.dataset.chain === state.chain;
        item.classList.toggle('active', active);
        item.setAttribute('aria-selected', active ? 'true' : 'false');
      });
      const url = new URL(window.location.href);
      url.searchParams.set('chain', state.chain);
      window.history.replaceState({}, '', url);
      await load();
      return;
    }
    const modeButton = event.target.closest('[data-mode]');
    if (modeButton) {
      state.mode = modeButton.dataset.mode;
      state.positionsPage = 1;
      state.closedPage = 1;
      document.querySelectorAll('.mode-tab').forEach((item) => item.classList.toggle('active', item.dataset.mode === state.mode));
      await load();
      return;
    }
    const copyButton = event.target.closest('[data-copy]');
    if (copyButton) {
      try { await navigator.clipboard.writeText(copyButton.dataset.copy); showToast('Mint 已复制'); } catch (_) { showToast('复制失败，请手动复制'); }
      return;
    }
    if (event.target.closest('#strategy-guide-btn') || event.target.closest('#strategy-guide-btn-inline')) openModal();
    if (event.target.closest('#strategy-modal-close') || event.target.closest('[data-modal-close]')) closeModal();
  });
  $('#closed-page-size').addEventListener('change', (event) => {
    const pageSize = Number(event.target.value);
    if (![10, 20, 50, 100].includes(pageSize)) return;
    state.closedPageSize = pageSize;
    state.closedPage = 1;
    renderClosedPositions(state.closedItems);
  });
  $('#price-display-unit').addEventListener('change', (event) => {
    state.priceUnit = event.target.value === 'native' ? 'native' : 'usd';
    renderChainLabels();
    renderClosedPositions(state.closedItems);
  });
  const filterControls = ['filter-strategy', 'filter-outcome', 'filter-status', 'filter-window'];
  filterControls.forEach((id) => {
    $(`#${id}`).addEventListener('change', async (event) => {
      state.filters[id.replace('filter-', '')] = event.target.value;
      state.positionsPage = 1;
      state.closedPage = 1;
      await load();
    });
  });
  $('#trend-window').addEventListener('change', async (event) => {
    state.trendWindow = event.target.value;
    await load();
  });
  let tokenFilterTimer = null;
  $('#filter-token').addEventListener('input', (event) => {
    state.filters.token = event.target.value;
    window.clearTimeout(tokenFilterTimer);
    tokenFilterTimer = window.setTimeout(() => { state.positionsPage = 1; state.closedPage = 1; load(); }, 260);
  });
  $('#filter-reset').addEventListener('click', async () => {
    state.trendWindow = '24h';
    state.filters = { token: '', strategy: 'all', outcome: 'all', status: 'all', window: '24h' };
    $('#filter-token').value = '';
    $('#filter-strategy').value = 'all';
    $('#filter-outcome').value = 'all';
    $('#filter-status').value = 'all';
    $('#filter-window').value = '24h';
    $('#trend-window').value = '24h';
    state.positionsPage = 1;
    state.closedPage = 1;
    await load();
    showToast('已重置筛选');
  });
  $('#refresh-btn').addEventListener('click', () => { load(); showToast('已手动刷新'); });
  document.addEventListener('keydown', (event) => { if (event.key === 'Escape') closeModal(); });
  renderChainLabels();
  load();
})();
