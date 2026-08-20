(() => {
  const $ = (id) => document.getElementById(id);
  const fmt = (value, digits = 8) => {
    if (value === null || value === undefined || value === "") return "—";
    const n = Number(value);
    return Number.isFinite(n) ? n.toLocaleString(undefined, { maximumFractionDigits: digits }) : String(value);
  };
  const pct = (value) => value === null || value === undefined ? "—" : `${fmt(value, 2)}%`;
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
  const cls = (value) => value === "HEALTHY" || value === "CONNECTED" || value === "RUNNING" ? "ok" : value === "OPEN" || value === "READY" ? "" : "warn";
  async function get(path, options) {
    const response = await fetch(path, { cache: "no-store", ...options });
    const data = await response.json();
    if (!response.ok) throw new Error(data.message || data.error || "请求失败");
    return data;
  }
  function card(label, value) { return `<div class="status-card"><div class="status-label">${label}</div><div class="status-value ${cls(value)}">${esc(value)}</div></div>`; }
  function renderStatus(status) {
    $("status").innerHTML = [
      card("Balanced Live", status.live), card("Bitget API", status.bitget), card("BNB余额", `${fmt(status.bnb_balance, 6)} BNB`),
      card("仓位容量", `Max ${status.max_open_positions} · Open ${status.positions}`), card("买入占位", status.buy_reserved),
      card("已用名额", status.used_slots), card("可用名额", status.available_slots), card("待退出", status.pending_exits), card("Main Loop", status.main_loop),
    ].join("");
    $("updated").textContent = `最后更新 ${status.last_updated || "—"}`;
    const notice = $("notice");
    if (status.bitget !== "CONNECTED") { notice.hidden = false; notice.textContent = "Bitget Wallet API不可用：持仓仍可查看，卖出按钮已禁用。"; }
    else if (status.wallet_error === "BITGET_BNB_BALANCE_INSUFFICIENT") { notice.hidden = false; notice.textContent = "BNB余额低于下单金额与Gas Reserve要求；已有仓位仍可卖出，新买入已暂停。"; }
    else { notice.hidden = true; }
  }
  async function copy(value) { try { await navigator.clipboard.writeText(value); toast("已复制"); } catch (_) { toast("复制失败"); } }
  function toast(message) { const node = $("toast"); node.hidden = false; node.textContent = message; setTimeout(() => { node.hidden = true; }, 1800); }
  function actionButton(position, percentage) {
    const disabled = !position.buttons_enabled || position.status === "SELL_PENDING" || position.status === "WAITING_CONFIRMATION" || position.status === "SWAP_UNKNOWN";
    const danger = percentage === 100 ? " danger" : "";
    return `<button class="${danger}" data-sell="${esc(position.position_id)}" data-pct="${percentage}" ${disabled ? "disabled" : ""}>卖出${percentage}%</button>`;
  }
  function renderPositions(data, status) {
    if (!data.items.length) { $("positions").innerHTML = `<div class="empty">当前没有真实 LIVE OPEN Position</div>`; return; }
    const bitgetReady = status.bitget === "CONNECTED";
    $("positions").innerHTML = data.items.map((p) => {
      const pnlClass = p.pnl_pct !== null && Number(p.pnl_pct) >= 0 ? "positive" : "negative";
      const priceClass = p.price_status === "PRICE_STALE" ? "stale" : "";
      const disabled = !bitgetReady || !p.buttons_enabled;
      const buttonPosition = {...p, buttons_enabled: !disabled};
      const sellButtons = [25, 50, 100].map((percentage) => actionButton(buttonPosition, percentage)).join("");
      const actions = p.retry_enabled ? `<div class="retry-note">上次失败，可重新卖出</div>${sellButtons}` : sellButtons;
      return `<article class="position">
        <div><div class="token-name">${esc(p.symbol)}</div><div class="token-address">${esc(p.mint_short)} <button class="copy" data-copy="${esc(p.mint)}">复制</button></div></div>
        <div><div class="metric-label">真实数量</div><div class="metric-value">${fmt(p.quantity, 4)}</div></div>
        <div><div class="metric-label">Entry / Current</div><div class="metric-value">${fmt(p.entry_price_native, 12)} / <span class="${priceClass}">${fmt(p.current_price_native, 12)}</span><div class="metric-label">Mark: ${esc(p.position_mark_source || p.position_price_freshness || "UNAVAILABLE")}</div></div></div>
        <div><div class="metric-label">持仓价值 BNB / USD</div><div class="metric-value">${fmt(p.value_bnb, 6)} / ${fmt(p.value_usd, 2)}</div></div>
        <div><div class="metric-label">PnL % / BNB</div><div class="metric-value ${pnlClass}">${pct(p.pnl_pct)} / ${fmt(p.pnl_bnb, 6)}</div></div>
        <div><div class="metric-label">最高收益 / 持仓时间</div><div class="metric-value">${pct(p.max_gain_pct)} / ${p.hold_seconds ?? "—"}s</div></div>
        <div class="actions">${actions}<div class="state ${p.status === "SWAP_UNKNOWN" || p.status === "ERROR" ? "bad" : ""}">${esc(p.status)}${p.exit_order_short ? ` · ${esc(p.exit_order_short)}` : ""}${p.price_status === "PRICE_STALE" ? " · PRICE STALE" : ""}</div></div>
      </article>`;
    }).join("");
    document.querySelectorAll("[data-copy]").forEach((node) => node.addEventListener("click", () => copy(node.dataset.copy)));
    document.querySelectorAll("[data-sell]").forEach((node) => node.addEventListener("click", () => sell(node.dataset.sell, Number(node.dataset.pct))));
  }
  function priceText(nativePrice, usdPrice) {
    if (usdPrice !== null && usdPrice !== undefined && usdPrice !== "") return `$${fmt(usdPrice, 12)}`;
    return nativePrice === null || nativePrice === undefined || nativePrice === "" ? "—" : `${fmt(nativePrice, 12)} BNB`;
  }
  function paired(entry, exit, formatter = (value) => fmt(value, 2)) {
    return `${formatter(entry)} / ${formatter(exit)}`;
  }
  function renderClosedPositions(data) {
    if (!data.items.length) { $("closed-positions").innerHTML = `<div class="empty">暂无已平仓记录</div>`; return; }
    $("closed-positions").innerHTML = `<table class="closed-table"><thead><tr>
      <th>代币</th><th>买入时间 / 卖出时间</th><th>买入价 / 卖出价</th><th>持币地址（买入 / 卖出）</th><th>流动性 USD（买入 / 卖出）</th><th>盈亏金额</th><th>盈亏率</th><th>退出原因</th>
    </tr></thead><tbody>${data.items.map((p) => {
      const pnlClass = p.pnl_pct !== null && Number(p.pnl_pct) >= 0 ? "positive" : "negative";
      return `<tr>
        <td><strong>${esc(p.symbol)}</strong><div class="token-address">${esc(p.mint_short)} <button class="copy" data-copy="${esc(p.mint)}">复制</button></div></td>
        <td>${esc(p.entry_at || "—")}<br>${esc(p.exit_at || "—")}</td>
        <td>${esc(priceText(p.entry_price_native, p.entry_price_usd))}<br>${esc(priceText(p.exit_price_native, p.exit_price_usd))}</td>
        <td>${esc(paired(p.entry_holders, p.exit_holders, (value) => fmt(value, 0)))}</td>
        <td>${esc(paired(p.entry_liquidity_usd, p.exit_liquidity_usd, (value) => `$${fmt(value, 2)}`))}</td>
        <td class="${pnlClass}">${p.pnl_currency === "USD" ? "$" : ""}${esc(fmt(p.pnl_amount, 8))}${p.pnl_currency === "BNB" ? " BNB" : ""}</td>
        <td class="${pnlClass}">${esc(pct(p.pnl_pct))}</td>
        <td>${esc(p.exit_reason || "—")}</td>
      </tr>`;
    }).join("")}</tbody></table>`;
    document.querySelectorAll("[data-copy]").forEach((node) => node.addEventListener("click", () => copy(node.dataset.copy)));
  }
  async function sell(positionId, percentage) {
    if (percentage === 100 && !window.confirm("确认全部卖出该 Token？")) return;
    document.querySelectorAll(`[data-sell="${CSS.escape(positionId)}"]`).forEach((node) => { node.disabled = true; });
    try { const result = await get(`/api/positions/${encodeURIComponent(positionId)}/sell`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ percentage }) }); toast(result.status === "FINISHED" ? "卖出已确认" : `卖出状态：${result.status}`); }
    catch (error) { toast(error.message); }
    await refresh();
  }
  async function refresh() {
    const button = $("refresh");
    button.disabled = true;
    try {
      const [status, positions, closedPositions] = await Promise.all([get("/api/status"), get("/api/positions"), get("/api/closed-positions")]);
      renderStatus(status); renderPositions(positions, status); renderClosedPositions(closedPositions);
    } catch (error) {
      $("notice").hidden = false; $("notice").textContent = `面板读取失败：${error.message}`;
    } finally {
      button.disabled = false;
    }
  }
  $("refresh").addEventListener("click", refresh);
  refresh();
})();
