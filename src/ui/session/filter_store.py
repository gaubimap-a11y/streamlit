from __future__ import annotations

from typing import Any

import streamlit as st


class FilterStore:
    def __init__(self, report_code: str) -> None:
        self._report_code = report_code

    def save(self, product_filter) -> None:
        st.session_state[self._filter_key] = self._serialize(product_filter)

    def load(self) -> dict[str, Any]:
        return dict(st.session_state.get(self._filter_key, {}))

    def detect_change_reset_page(self, current) -> bool:
        serialized = self._serialize(current)
        if serialized != st.session_state.get(self._filter_key):
            self.set_page(1)
            return True
        return False

    def get_page(self) -> int:
        return int(st.session_state.get(self._page_key, 1))

    def set_page(self, page: int) -> None:
        st.session_state[self._page_key] = max(1, int(page))

    def clear(self) -> None:
        st.session_state.pop(self._filter_key, None)
        st.session_state.pop(self._page_key, None)

    def replace_payload(self, payload) -> None:
        st.session_state[self._filter_key] = self._serialize(payload)
        self.set_page(1)

    @property
    def _filter_key(self) -> str:
        return f"_{self._report_code}_filter"

    @property
    def _page_key(self) -> str:
        return f"_{self._report_code}_page"

    def _serialize(self, product_filter) -> dict[str, Any]:
        if product_filter is None:
            return {}
        if hasattr(product_filter, "model_dump"):
            return product_filter.model_dump()
        if isinstance(product_filter, dict):
            return dict(product_filter)
        raise TypeError("Unsupported filter payload.")
