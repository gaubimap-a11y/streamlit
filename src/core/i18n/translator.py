from __future__ import annotations

import json
import os
from typing import Any

import streamlit as st


class Translator:
    _translations: dict[str, dict[str, Any]] = {}
    _locales_dir = os.path.join(os.path.dirname(__file__), "locales")

    @classmethod
    def load_translation(cls, locale: str) -> None:
        if locale not in cls._translations:
            file_path = os.path.join(cls._locales_dir, f"{locale}.json")
            if os.path.exists(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        cls._translations[locale] = json.load(f)
                except Exception:
                    cls._translations[locale] = {}
            else:
                cls._translations[locale] = {}

    @classmethod
    def translate(cls, key_path: str, locale: str | None = None) -> str:
        if locale is None:
            # Default to session state or Vietnamese
            locale = st.session_state.get("locale", "vi")
            # Handle cases where session state might have a full name instead of code
            if locale == "Tiếng Việt": locale = "vi"
            if locale == "日本語": locale = "ja"
        
        cls.load_translation(locale)
        
        keys = key_path.split(".")
        val = cls._translations.get(locale, {})
        for k in keys:
            if isinstance(val, dict):
                val = val.get(k, key_path)
            else:
                return key_path
        
        return str(val) if not isinstance(val, dict) else key_path


def t(key_path: str) -> str:
    """Helper function for quick translation access."""
    return Translator.translate(key_path)
