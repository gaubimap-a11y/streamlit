from __future__ import annotations

from html import escape

import streamlit as st

from src.core.i18n.translator import t


def render_metric_cards(cards: list[dict[str, str]]) -> None:
    columns = st.columns(len(cards))
    for column, card in zip(columns, cards):
        tone = escape(card.get("tone", "neutral"))
        delta = escape(card.get("delta", ""))
        with column:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <span class="kpi-label">{escape(card["label"])}</span>
                    <div class="kpi-value">{escape(card["value"])}</div>
                    <span class="delta-pill {tone}">{delta}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_welcome_ui() -> None:
    render_metric_cards(
        [
            {
                "label": t("ui.dashboard.metrics.db_connected"),
                "value": "Databricks",
                "delta": f"↑ {t('ui.dashboard.metrics.active')}",
                "tone": "positive",
            },
            {
                "label": t("ui.dashboard.metrics.avg_latency"),
                "value": "1.2s",
                "delta": "↓ -15%",
                "tone": "negative",
            },
            {
                "label": t("ui.dashboard.metrics.data_freshness"),
                "value": t("ui.dashboard.metrics.realtime"),
                "delta": "↑ 100%",
                "tone": "positive",
            },
        ]
    )
