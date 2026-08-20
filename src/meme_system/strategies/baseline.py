"""The frozen Solana ultra-early baseline strategy.

This module evaluates only local, already-normalized inputs. It has no network
access and no transaction execution path.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.domain.models import (
    BSC_BASELINE_IDENTITY,
    BASELINE_IDENTITY,
    CostBreakdown,
    EntryDecision,
    EntryFeatures,
    ExitDecision,
    RuleCheck,
    ShadowExitFeatures,
    StrategyIdentity,
    VirtualPosition,
)


ZERO = Decimal("0")
SOLANA_SHADOW_MIN_LIQUIDITY_USD = Decimal("5000")


@dataclass(frozen=True)
class BaselineConfig:
    identity: StrategyIdentity = BASELINE_IDENTITY
    observation_delay_sec: int = 15
    # Solana records these fields but does not reject on them.  BSC explicitly
    # enables its existing enforcement in bsc_baseline_config().
    enforce_holders: bool = False
    enforce_market_cap: bool = False
    enforce_liquidity: bool = False
    require_observation_price: bool = False
    require_observation_liquidity: bool = False
    require_holders_non_decreasing_after_observation: bool = False
    token_age_min_sec: int = 5
    token_age_max_sec: int = 120
    unique_buyers_15s_min: int = 6
    buy_sell_count_ratio_15s_min: Decimal = Decimal("1.8")
    require_two_non_negative_flow_windows: bool = True
    require_executable_buy_route: bool = True
    require_executable_sell_route: bool = True
    creator_confirmed_sold_at_entry: bool = False
    max_buy_price_impact_pct: Decimal = Decimal("10")
    max_immediate_exit_impact_pct: Decimal = Decimal("15")
    one_trade_per_mint: bool = True
    same_name_cooldown_sec: int = 900
    max_open_positions: int = 50
    min_holders: int = 20
    min_holders_inclusive: bool = False
    min_market_cap_usd: Decimal = Decimal("1000")
    min_liquidity_usd: Decimal = Decimal("100")
    initial_virtual_balance_sol: Decimal = Decimal("1")
    position_size_sol: Decimal = Decimal("0.001")
    take_profit_pct: Decimal = Decimal("0.10")
    stop_loss_trigger_pct: Decimal = Decimal("-0.20")
    partial_take_profit_enabled: bool = False
    take_profit_1_pct: Decimal = Decimal("0.10")
    take_profit_2_pct: Decimal = Decimal("0.10")
    partial_take_profit_sell_pct: Decimal = Decimal("1")
    tp2_breakeven_exit_enabled: bool = False
    max_hold_sec: int = 600
    daily_full_loss_sol_limit: Decimal = Decimal("0.01")
    pause_new_entries_after_large_losses: int = 50
    large_loss_threshold_pct: Decimal = Decimal("-0.40")
    shadow_defense_pct: Decimal = Decimal("-0.08")
    shadow_time_sec: int = 120
    shadow_mfe_pct: Decimal = Decimal("0.05")
    shadow_holders_drop_pct: Decimal = Decimal("0.10")
    shadow_liquidity_drop_pct: Decimal = Decimal("0.15")


def solana_baseline_config() -> BaselineConfig:
    """Return the single Solana Paper/Shadow entry configuration.

    These are the existing runner values, centralized so the runner and
    Dashboard cannot silently advertise different controls.  Holders is the
    configured Solana hard gate; the remaining market-data fields stay
    record-only unless explicitly enabled.
    """
    return BaselineConfig(
        enforce_holders=True,
        min_holders=20,
        min_holders_inclusive=False,
        pause_new_entries_after_large_losses=1000,
        take_profit_pct=Decimal("0.30"),
        stop_loss_trigger_pct=Decimal("-0.30"),
        partial_take_profit_enabled=True,
        take_profit_1_pct=Decimal("0.20"),
        take_profit_2_pct=Decimal("0.30"),
        partial_take_profit_sell_pct=Decimal("0.50"),
        tp2_breakeven_exit_enabled=True,
    )


def bsc_baseline_config() -> BaselineConfig:
    """Return the data-backed BSC Paper/Shadow overlay.

    This is intentionally a BSC-only configuration.  Solana continues to use
    the frozen ``BaselineConfig`` defaults and Jupiter quote requirements.
    """
    from meme_system.domain.models import BSC_BASELINE_IDENTITY

    return BaselineConfig(
        identity=BSC_BASELINE_IDENTITY,
        observation_delay_sec=60,
        enforce_holders=True,
        enforce_market_cap=True,
        enforce_liquidity=True,
        require_observation_price=True,
        require_observation_liquidity=True,
        require_holders_non_decreasing_after_observation=True,
        min_holders=100,
        min_holders_inclusive=True,
        initial_virtual_balance_sol=Decimal("0.1"),
        position_size_sol=Decimal("0.01"),
        stop_loss_trigger_pct=Decimal("-0.10"),
        shadow_holders_drop_pct=Decimal("0.10"),
        shadow_liquidity_drop_pct=Decimal("0.15"),
    )


class BaselineStrategy:
    def __init__(self, config: BaselineConfig | None = None) -> None:
        self.config = config or BaselineConfig()

    def _local_entry_checks(self, features: EntryFeatures) -> list[RuleCheck]:
        checks: list[RuleCheck] = []
        checks.append(
            self._optional_numeric_check(
                "token_age",
                features.token_age_sec,
                f"{self.config.token_age_min_sec}..{self.config.token_age_max_sec}",
                lambda value: self.config.token_age_min_sec <= value <= self.config.token_age_max_sec,
                "token_age_unavailable",
                "币龄不可用；本项仅记录，不阻断入场",
                "token_age_out_of_range",
                "币龄不在 5–120 秒范围",
            )
        )
        checks.append(
            self._optional_numeric_check(
                "unique_buyers_15s",
                features.unique_buyers_15s,
                self.config.unique_buyers_15s_min,
                lambda value: value >= self.config.unique_buyers_15s_min,
                "unique_buyers_unavailable",
                "15 秒独立买家数不可用；本项仅记录，不阻断入场",
                "unique_buyers_below_min",
                "15 秒独立买家数不足",
            )
        )
        checks.append(
            self._optional_numeric_check(
                "buy_sell_count_ratio_15s",
                features.buy_sell_count_ratio_15s,
                self.config.buy_sell_count_ratio_15s_min,
                lambda value: value >= self.config.buy_sell_count_ratio_15s_min,
                "buy_sell_ratio_unavailable",
                "15 秒买卖笔数比不可用；本项仅记录，不阻断入场",
                "buy_sell_ratio_below_min",
                "15 秒买卖笔数比不足",
            )
        )
        checks.append(
            self._optional_numeric_check(
                "net_buy_15s",
                features.net_buy_15s,
                "> 0",
                lambda value: value > ZERO,
                "net_buy_unavailable",
                "15 秒净买入不可用；本项仅记录，不阻断入场",
                "net_buy_not_positive",
                "15 秒净买入不为正",
            )
        )
        checks.append(
            self._optional_flow_check(features.flow_windows_non_negative)
        )
        checks.append(
            self._optional_creator_check(features.creator_confirmed_sold)
        )
        checks.append(self._holders_check(features.holders))
        checks.append(self._market_cap_check(features.market_cap_usd))
        checks.append(self._liquidity_check(features.liquidity_usd))
        return checks

    def local_entry_eligible(self, features: EntryFeatures) -> bool:
        """Check all non-quote gates before an expensive BSC quote request."""

        return all(check.passed for check in self._local_entry_checks(features))

    def evaluate_local_entry(self, features: EntryFeatures) -> EntryDecision:
        """Record a pre-quote decision without fabricating quote failures."""

        checks = self._local_entry_checks(features)
        return EntryDecision(
            accepted=all(check.passed for check in checks),
            identity=self.config.identity,
            checks=tuple(checks),
            soft_features=dict(features.soft_features or {}),
        )

    def evaluate_entry(self, features: EntryFeatures) -> EntryDecision:
        checks = self._local_entry_checks(features)
        if features.pricing_mode == "binance_indicative":
            checks.append(
                self._check(
                    "binance_indicative_price",
                    features.pricing_error is None
                    and features.buy_quote is not None
                    and features.sell_quote is not None,
                    features.pricing_error or "available",
                    "valid Binance current price",
                    "bsc_price_unavailable",
                    "Binance 当前价格缺失或无效，拒绝开仓",
                )
            )
        elif features.pricing_mode == "bsc_executable_quote" and not bool(
            (features.soft_features or {}).get("quote_requested", True)
        ):
            # Realtime defers BSC chain quotes until the local gates pass.
            # A candidate which was never quoted must not be displayed as a
            # buy/sell quote failure.
            pass
        elif features.pricing_error in {
            "venue_unrecognized",
            "fourmeme_context_unavailable",
            "unsupported_fundraising_asset",
            "flap_context_unavailable",
            "flap_buy_quote_unavailable",
            "flap_sell_quote_unavailable",
            "bonding_curve_buy_quote_unavailable",
            "bonding_curve_sell_quote_unavailable",
            "pancakeswap_quote_unavailable",
        }:
            checks.append(
                self._check(
                    "bsc_quote_context",
                    False,
                    features.pricing_error,
                    "verified executable quote",
                    features.pricing_error,
                    "BSC 链上报价不可用：" + features.pricing_error,
                )
            )
        else:
            checks.append(
                self._quote_check(
                    "buy_route",
                    features.buy_quote,
                    self.config.position_size_sol,
                    features.evaluated_at,
                    self.config.max_buy_price_impact_pct,
                    "buy",
                )
            )
            expected_sell_quantity = (
                features.buy_quote.output_quantity if features.buy_quote is not None else None
            )
            checks.append(
                self._quote_check(
                    "sell_route",
                    features.sell_quote,
                    expected_sell_quantity,
                    features.evaluated_at,
                    self.config.max_immediate_exit_impact_pct,
                    "sell",
                )
            )
        return EntryDecision(
            accepted=all(check.passed for check in checks),
            identity=self.config.identity,
            checks=tuple(checks),
            soft_features=dict(features.soft_features or {}),
        )

    def _holders_check(self, holders: int | None) -> RuleCheck:
        if not self.config.enforce_holders:
            return RuleCheck(
                name="holders",
                passed=True,
                actual=holders,
                threshold=f"record_only ({self._holders_threshold()})",
                reason_code=None,
                reason_zh="持币地址数仅记录，不阻断入场",
            )
        if holders is None:
            return RuleCheck(
                name="holders",
                passed=False,
                actual=None,
                threshold=self._holders_threshold(),
                reason_code="holders_unavailable",
                reason_zh="入场时持币地址数缺失，拒绝开仓",
            )
        passed = holders >= self.config.min_holders if self.config.min_holders_inclusive else holders > self.config.min_holders
        return RuleCheck(
            name="holders",
            passed=passed,
            actual=holders,
            threshold=self._holders_threshold(),
            reason_code=None if passed else "holders_below_min",
            reason_zh=None if passed else "入场时持币地址数低于最低值",
        )

    def _holders_threshold(self) -> str:
        operator = ">=" if self.config.min_holders_inclusive else ">"
        return f"{operator} {self.config.min_holders}"

    def _market_cap_check(self, market_cap_usd: Decimal | None) -> RuleCheck:
        if not self.config.enforce_market_cap:
            return RuleCheck(
                name="market_cap_usd",
                passed=True,
                actual=market_cap_usd,
                threshold=f"record_only (>= {self.config.min_market_cap_usd} USD)",
                reason_code=None,
                reason_zh="市值仅记录，不阻断入场",
            )
        if market_cap_usd is None:
            return RuleCheck(
                name="market_cap_usd",
                passed=False,
                actual=None,
                threshold=f">= {self.config.min_market_cap_usd} USD",
                reason_code="market_cap_unavailable",
                reason_zh="入场时 Binance market_cap 缺失，拒绝开仓",
            )
        return RuleCheck(
            name="market_cap_usd",
            passed=market_cap_usd >= self.config.min_market_cap_usd,
            actual=market_cap_usd,
            threshold=f">= {self.config.min_market_cap_usd} USD",
            reason_code=(
                None
                if market_cap_usd >= self.config.min_market_cap_usd
                else "market_cap_below_min"
            ),
            reason_zh=(
                None
                if market_cap_usd >= self.config.min_market_cap_usd
                else "入场时 Binance market_cap 低于最低值"
            ),
        )

    def _liquidity_check(self, liquidity_usd: Decimal | None) -> RuleCheck:
        if not self.config.enforce_liquidity:
            return RuleCheck(
                name="liquidity_usd",
                passed=True,
                actual=liquidity_usd,
                threshold=f"record_only (>= {self.config.min_liquidity_usd} USD)",
                reason_code=None,
                reason_zh="流动性仅记录，不阻断入场",
            )
        if liquidity_usd is None:
            return RuleCheck(
                name="liquidity_usd",
                passed=False,
                actual=None,
                threshold=f">= {self.config.min_liquidity_usd} USD",
                reason_code="liquidity_unavailable",
                reason_zh="入场时 Binance liquidity 缺失，拒绝开仓",
            )
        return RuleCheck(
            name="liquidity_usd",
            passed=liquidity_usd >= self.config.min_liquidity_usd,
            actual=liquidity_usd,
            threshold=f">= {self.config.min_liquidity_usd} USD",
            reason_code=(
                None
                if liquidity_usd >= self.config.min_liquidity_usd
                else "liquidity_below_min"
            ),
            reason_zh=(
                None
                if liquidity_usd >= self.config.min_liquidity_usd
                else "入场时 Binance liquidity 低于最低值"
            ),
        )

    def evaluate_paper_exit(
        self,
        position: VirtualPosition,
        now: datetime,
        sell_quote: ExecutableQuote | None,
        estimated_network_fee_sol: Decimal | None = None,
        estimated_priority_fee_sol: Decimal | None = None,
    ) -> ExitDecision:
        age_sec = max(0, int((now - position.opened_at).total_seconds()))
        quote_error = self._validate_sell_quote(position, sell_quote, now)
        if quote_error is not None:
            return ExitDecision(
                triggered=False,
                identity=position.identity,
                reason=quote_error,
                position_age_sec=age_sec,
                return_pct=None,
                quote=sell_quote,
                cost=None,
            )

        assert sell_quote is not None
        cost = self._cost_breakdown(
            position,
            sell_quote,
            estimated_network_fee_sol,
            estimated_priority_fee_sol,
        )
        price_return_pct = self._price_return_pct(position, sell_quote)
        reason = self._paper_exit_reason(position, price_return_pct, age_sec)
        return ExitDecision(
            triggered=reason is not None,
            identity=position.identity,
            reason=reason,
            position_age_sec=age_sec,
            return_pct=price_return_pct,
            quote=sell_quote,
            cost=cost,
        )

    def evaluate_shadow_exit(
        self,
        position: VirtualPosition,
        features: ShadowExitFeatures,
        now: datetime,
        sell_quote: ExecutableQuote | None,
        estimated_network_fee_sol: Decimal | None = None,
        estimated_priority_fee_sol: Decimal | None = None,
    ) -> ExitDecision:
        reason: str | None = None
        if self._relative_drop_exceeds(
            position.entry_holders,
            features.holders,
            self.config.shadow_holders_drop_pct,
        ):
            reason = "shadow_holders_drop_over_10pct"
        elif self._relative_drop_exceeds(
            position.entry_liquidity_usd,
            features.liquidity_usd,
            self.config.shadow_liquidity_drop_pct,
        ):
            reason = "shadow_liquidity_drop_over_15pct"
        elif (
            features.return_pct <= self.config.shadow_defense_pct
            and features.recent_net_flow_negative
            and features.independent_buyer_growth_stopped
        ):
            reason = "shadow_defense_v1"
        elif features.creator_sell_confident:
            reason = "shadow_creator_sell"
        elif (
            features.position_age_sec >= self.config.shadow_time_sec
            and features.mfe_pct < self.config.shadow_mfe_pct
            and features.buyer_growth_and_flow_slowed
        ):
            reason = "shadow_time_exit"

        # Shadow keeps its structural early-exit overlay, but its standard
        # quote-based TP/SL/timeout rules are the same as Paper.  This keeps
        # the comparison lifecycle complete instead of treating TP/SL as
        # observation-only metrics.
        cost: CostBreakdown | None = None
        if reason is None:
            quote_error = self._validate_sell_quote(position, sell_quote, now)
            if quote_error is not None:
                return ExitDecision(
                    triggered=False,
                    identity=position.identity,
                    reason=quote_error,
                    position_age_sec=features.position_age_sec,
                    return_pct=None,
                    quote=sell_quote,
                    cost=None,
                )
            assert sell_quote is not None
            cost = self._cost_breakdown(
                position,
                sell_quote,
                estimated_network_fee_sol,
                estimated_priority_fee_sol,
            )
            reason = self._paper_exit_reason(
                position,
                self._price_return_pct(position, sell_quote),
                features.position_age_sec,
            )

        if reason is None:
            return ExitDecision(
                triggered=False,
                identity=position.identity,
                reason=None,
                position_age_sec=features.position_age_sec,
                return_pct=(
                    self._price_return_pct(position, sell_quote)
                    if cost is not None and sell_quote is not None
                    else features.return_pct
                ),
                quote=sell_quote,
                cost=cost,
            )

        if cost is None:
            quote_error = self._validate_sell_quote(position, sell_quote, now)
            if quote_error is not None:
                return ExitDecision(
                    triggered=True,
                    identity=position.identity,
                    reason=quote_error,
                    position_age_sec=features.position_age_sec,
                    return_pct=features.return_pct,
                    quote=sell_quote,
                    cost=None,
                )
            assert sell_quote is not None
            cost = self._cost_breakdown(
                position,
                sell_quote,
                estimated_network_fee_sol,
                estimated_priority_fee_sol,
            )

        assert cost is not None
        return ExitDecision(
            triggered=True,
            identity=position.identity,
            reason=reason,
            position_age_sec=features.position_age_sec,
            return_pct=self._price_return_pct(position, sell_quote),
            quote=sell_quote,
            cost=cost,
        )

    def _paper_exit_reason(
        self,
        position: VirtualPosition,
        price_return_pct: Decimal,
        age_sec: int,
    ) -> str | None:
        if price_return_pct <= self.config.stop_loss_trigger_pct:
            return "stop_loss"
        if (
            self.config.partial_take_profit_enabled
            and position.tp2_executed_at is not None
            and self.config.tp2_breakeven_exit_enabled
            and price_return_pct <= ZERO
        ):
            return "tp2_breakeven_exit"
        if self.config.partial_take_profit_enabled:
            if position.tp1_executed_at is None and price_return_pct >= self.config.take_profit_1_pct:
                return "take_profit_1"
            if (
                position.tp1_executed_at is not None
                and position.tp2_executed_at is None
                and price_return_pct >= self.config.take_profit_2_pct
            ):
                return "take_profit_2"
        elif price_return_pct >= self.config.take_profit_pct:
            return "take_profit"
        if age_sec >= self.config.max_hold_sec:
            return "max_hold_timeout"
        return None

    @staticmethod
    def _relative_drop_exceeds(
        entry_value: Decimal | int | None,
        current_value: Decimal | int | None,
        threshold: Decimal,
    ) -> bool:
        if threshold <= ZERO or entry_value is None or current_value is None:
            return False
        entry = Decimal(str(entry_value))
        current = Decimal(str(current_value))
        if not entry.is_finite() or not current.is_finite() or entry <= ZERO or current < ZERO:
            return False
        return (entry - current) / entry > threshold

    def _check(
        self,
        name: str,
        passed: bool,
        actual: object,
        threshold: object,
        reason_code: str,
        reason_zh: str,
    ) -> RuleCheck:
        return RuleCheck(
            name=name,
            passed=passed,
            actual=actual,
            threshold=threshold,
            reason_code=None if passed else reason_code,
            reason_zh=None if passed else reason_zh,
        )

    def _optional_numeric_check(
        self,
        name: str,
        actual: Decimal | int | None,
        threshold: object,
        predicate,
        unavailable_code: str,
        unavailable_zh: str,
        failed_code: str,
        failed_zh: str,
    ) -> RuleCheck:
        if actual is None:
            return RuleCheck(name, True, None, threshold, unavailable_code, unavailable_zh)
        return self._check(name, predicate(actual), actual, threshold, failed_code, failed_zh)

    def _optional_flow_check(
        self,
        actual: tuple[bool | None, bool | None],
    ) -> RuleCheck:
        if any(value is None for value in actual):
            return RuleCheck(
                "two_non_negative_flow_windows",
                True,
                actual,
                (True, True),
                "flow_window_unavailable",
                "短窗口净流量不可用；本项仅记录，不阻断入场",
            )
        return self._check(
            "two_non_negative_flow_windows",
            all(value is True for value in actual),
            actual,
            (True, True),
            "flow_window_negative",
            "两个短窗口中存在非正净流量",
        )

    def _optional_creator_check(self, actual: bool | None) -> RuleCheck:
        if actual is None:
            return RuleCheck(
                "creator_not_confirmed_sold",
                True,
                None,
                self.config.creator_confirmed_sold_at_entry,
                "creator_sell_unavailable",
                "创建者卖出状态不可用；本项仅记录，不阻断入场",
            )
        return self._check(
            "creator_not_confirmed_sold",
            actual is self.config.creator_confirmed_sold_at_entry,
            actual,
            self.config.creator_confirmed_sold_at_entry,
            "creator_confirmed_sold",
            "入场时已确认创建者卖出",
        )

    def _quote_check(
        self,
        name: str,
        quote: ExecutableQuote | None,
        expected_input: Decimal | None,
        now: datetime,
        max_impact_pct: Decimal,
        side: str,
    ) -> RuleCheck:
        if quote is None:
            return self._check(
                name,
                False,
                None,
                "executable",
                f"{side}_quote_unavailable",
                f"{side} 方向没有可执行报价",
            )
        error = quote.unusable_reason(now)
        if error is not None:
            return self._check(
                name,
                False,
                error,
                "executable",
                error,
                f"{side} 报价不可执行：{error}",
            )
        if quote.side != side:
            return self._check(
                name,
                False,
                quote.side,
                side,
                f"{side}_quote_side_mismatch",
                f"{side} 报价方向不一致",
            )
        if expected_input is not None and quote.input_quantity != expected_input:
            return self._check(
                name,
                False,
                quote.input_quantity,
                expected_input,
                f"{side}_quote_quantity_mismatch",
                f"{side} 报价数量与预期不一致",
            )
        if quote.output_quantity <= ZERO:
            return self._check(
                name,
                False,
                quote.output_quantity,
                "> 0",
                f"{side}_quote_output_unavailable",
                f"{side} 报价 outAmount 必须大于 0",
            )
        if quote.price_impact_pct is None:
            return RuleCheck(
                name,
                True,
                None,
                max_impact_pct,
                f"{side}_price_impact_unavailable",
                f"{side} 报价 price impact 不可用；本项仅记录，不阻断入场",
            )
        return self._check(
            name,
            quote.price_impact_pct <= max_impact_pct,
            quote.price_impact_pct,
            max_impact_pct,
            f"{side}_price_impact_too_high",
            f"{side} price impact 超过上限",
        )

    def _validate_sell_quote(
        self,
        position: VirtualPosition,
        sell_quote: ExecutableQuote | None,
        now: datetime,
    ) -> str | None:
        if sell_quote is None:
            return "sell_quote_unavailable"
        error = sell_quote.unusable_reason(now)
        if error is not None:
            return error
        if sell_quote.input_quantity != position.active_quantity_token:
            return "sell_quote_quantity_mismatch"
        return None

    def _cost_breakdown(
        self,
        position: VirtualPosition,
        sell_quote: ExecutableQuote,
        estimated_network_fee_sol: Decimal | None,
        estimated_priority_fee_sol: Decimal | None,
    ) -> CostBreakdown:
        gross_proceeds = position.realized_proceeds_sol + sell_quote.output_quantity
        gross_pnl_sol = gross_proceeds - position.quantity_sol
        gross_pnl_pct = gross_pnl_sol / position.quantity_sol
        route_fee = position.realized_route_fee_sol + (sell_quote.route_fee or ZERO)
        network_fee = position.realized_network_fee_sol + (estimated_network_fee_sol or ZERO)
        priority_fee = position.realized_priority_fee_sol + (estimated_priority_fee_sol or ZERO)
        known_costs = route_fee + network_fee + priority_fee
        return CostBreakdown(
            gross_pnl_sol=gross_pnl_sol,
            gross_pnl_pct=gross_pnl_pct,
            route_fee_sol=route_fee,
            estimated_network_fee_sol=network_fee,
            estimated_priority_fee_sol=priority_fee,
            net_pnl_estimated_sol=gross_pnl_sol - known_costs,
            net_pnl_is_estimated=(
                sell_quote.route_fee is None
                or estimated_network_fee_sol is None
                or estimated_priority_fee_sol is None
            ),
        )

    @staticmethod
    def _price_return_pct(position: VirtualPosition, sell_quote: ExecutableQuote) -> Decimal:
        """Mark the current unit price against entry, excluding prior TP proceeds."""
        if (
            position.entry_quantity_token <= ZERO
            or position.quantity_sol <= ZERO
            or sell_quote.input_quantity <= ZERO
        ):
            return ZERO
        marked_value = (
            sell_quote.output_quantity
            * position.entry_quantity_token
            / sell_quote.input_quantity
        )
        return (marked_value - position.quantity_sol) / position.quantity_sol
