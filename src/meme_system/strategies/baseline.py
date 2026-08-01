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


@dataclass(frozen=True)
class BaselineConfig:
    identity: StrategyIdentity = BASELINE_IDENTITY
    token_age_min_sec: int = 5
    token_age_max_sec: int = 120
    unique_buyers_15s_min: int = 6
    buy_sell_count_ratio_15s_min: Decimal = Decimal("1.8")
    require_two_non_negative_flow_windows: bool = True
    require_executable_buy_route: bool = True
    require_executable_sell_route: bool = True
    creator_confirmed_sold_at_entry: bool = False
    max_buy_price_impact_pct: Decimal = Decimal("3")
    max_immediate_exit_impact_pct: Decimal = Decimal("8")
    one_trade_per_mint: bool = True
    same_name_cooldown_sec: int = 900
    max_open_positions: int = 2
    initial_virtual_balance_sol: Decimal = Decimal("10")
    position_size_sol: Decimal = Decimal("0.001")
    take_profit_pct: Decimal = Decimal("0.10")
    stop_loss_trigger_pct: Decimal = Decimal("-0.20")
    max_hold_sec: int = 600
    daily_full_loss_units_limit: int = 5
    pause_new_entries_after_large_losses: int = 3
    large_loss_threshold_pct: Decimal = Decimal("-0.40")
    shadow_defense_pct: Decimal = Decimal("-0.08")
    shadow_time_sec: int = 120
    shadow_mfe_pct: Decimal = Decimal("0.05")


class BaselineStrategy:
    def __init__(self, config: BaselineConfig | None = None) -> None:
        self.config = config or BaselineConfig()

    def evaluate_entry(self, features: EntryFeatures) -> EntryDecision:
        checks: list[RuleCheck] = []
        checks.append(
            self._check(
                "token_age",
                features.token_age_sec is not None
                and self.config.token_age_min_sec <= features.token_age_sec <= self.config.token_age_max_sec,
                features.token_age_sec,
                f"{self.config.token_age_min_sec}..{self.config.token_age_max_sec}",
                "token_age_unavailable" if features.token_age_sec is None else "token_age_out_of_range",
                "币龄不可用" if features.token_age_sec is None else "币龄不在 5–120 秒范围",
            )
        )
        checks.append(
            self._check(
                "unique_buyers_15s",
                features.unique_buyers_15s is not None
                and features.unique_buyers_15s >= self.config.unique_buyers_15s_min,
                features.unique_buyers_15s,
                self.config.unique_buyers_15s_min,
                "unique_buyers_unavailable" if features.unique_buyers_15s is None else "unique_buyers_below_min",
                "15 秒独立买家数不可用" if features.unique_buyers_15s is None else "15 秒独立买家数不足",
            )
        )
        checks.append(
            self._check(
                "buy_sell_count_ratio_15s",
                features.buy_sell_count_ratio_15s is not None
                and features.buy_sell_count_ratio_15s >= self.config.buy_sell_count_ratio_15s_min,
                features.buy_sell_count_ratio_15s,
                self.config.buy_sell_count_ratio_15s_min,
                "buy_sell_ratio_unavailable" if features.buy_sell_count_ratio_15s is None else "buy_sell_ratio_below_min",
                "15 秒买卖笔数比不可用" if features.buy_sell_count_ratio_15s is None else "15 秒买卖笔数比不足",
            )
        )
        checks.append(
            self._check(
                "net_buy_15s",
                features.net_buy_15s is not None and features.net_buy_15s > ZERO,
                features.net_buy_15s,
                "> 0",
                "net_buy_unavailable" if features.net_buy_15s is None else "net_buy_not_positive",
                "15 秒净买入不可用" if features.net_buy_15s is None else "15 秒净买入不为正",
            )
        )
        checks.append(
            self._check(
                "two_non_negative_flow_windows",
                all(value is True for value in features.flow_windows_non_negative),
                features.flow_windows_non_negative,
                (True, True),
                "flow_window_unavailable" if any(value is None for value in features.flow_windows_non_negative) else "flow_window_negative",
                "短窗口净流量不可用" if any(value is None for value in features.flow_windows_non_negative) else "两个短窗口中存在非正净流量",
            )
        )
        checks.append(
            self._check(
                "creator_not_confirmed_sold",
                features.creator_confirmed_sold is not None
                and features.creator_confirmed_sold is self.config.creator_confirmed_sold_at_entry,
                features.creator_confirmed_sold,
                self.config.creator_confirmed_sold_at_entry,
                "creator_sell_unavailable" if features.creator_confirmed_sold is None else "creator_confirmed_sold",
                "创建者卖出状态不可用" if features.creator_confirmed_sold is None else "入场时已确认创建者卖出",
            )
        )
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
        if cost.gross_pnl_pct <= self.config.stop_loss_trigger_pct:
            reason = "stop_loss"
        elif cost.gross_pnl_pct >= self.config.take_profit_pct:
            reason = "take_profit"
        elif age_sec >= self.config.max_hold_sec:
            reason = "max_hold_timeout"
        else:
            reason = None
        return ExitDecision(
            triggered=reason is not None,
            identity=position.identity,
            reason=reason,
            position_age_sec=age_sec,
            return_pct=cost.gross_pnl_pct,
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
        if (
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

        if reason is None:
            return ExitDecision(
                triggered=False,
                identity=position.identity,
                reason=None,
                position_age_sec=features.position_age_sec,
                return_pct=features.return_pct,
                quote=sell_quote,
                cost=None,
            )

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
        return ExitDecision(
            triggered=True,
            identity=position.identity,
            reason=reason,
            position_age_sec=features.position_age_sec,
            return_pct=features.return_pct,
            quote=sell_quote,
            cost=self._cost_breakdown(
                position,
                sell_quote,
                estimated_network_fee_sol,
                estimated_priority_fee_sol,
            ),
        )

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
        if quote.price_impact_pct is None:
            return self._check(
                name,
                False,
                None,
                max_impact_pct,
                f"{side}_price_impact_unavailable",
                f"{side} 报价缺少 price impact",
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
        gross_pnl_sol = sell_quote.output_quantity - position.quantity_sol
        gross_pnl_pct = gross_pnl_sol / position.quantity_sol
        route_fee = sell_quote.route_fee
        known_costs = (route_fee or ZERO) + (
            estimated_network_fee_sol or ZERO
        ) + (estimated_priority_fee_sol or ZERO)
        return CostBreakdown(
            gross_pnl_sol=gross_pnl_sol,
            gross_pnl_pct=gross_pnl_pct,
            route_fee_sol=route_fee,
            estimated_network_fee_sol=estimated_network_fee_sol,
            estimated_priority_fee_sol=estimated_priority_fee_sol,
            net_pnl_estimated_sol=gross_pnl_sol - known_costs,
            net_pnl_is_estimated=(
                route_fee is None
                or estimated_network_fee_sol is None
                or estimated_priority_fee_sol is None
            ),
        )
