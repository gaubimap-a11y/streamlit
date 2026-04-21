from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components


_STYLES_DIR = Path(__file__).resolve().parent


def _is_pytest_runtime() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))


def load_css(filename: str) -> str:
    return (_STYLES_DIR / filename).read_text(encoding="utf-8")


def inject_css(*filenames: str) -> None:
    if not filenames:
        return
    css = "\n".join(load_css(filename) for filename in filenames)
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def sync_theme_mode(dark_mode: bool) -> None:
    if _is_pytest_runtime():
        return

    components.html(
        """
        <script>
        (function() {
          const value = "__DARK__";
          const apply = (doc) => {
            if (!doc || !doc.documentElement) return;
            doc.documentElement.setAttribute("data-ui-dark", value);
          };

          // Streamlit components run inside an iframe. Apply the attribute to the
          // parent document (main app) when possible, and fallback to the iframe.
          try { apply(window.parent.document); } catch (e) {}
          try { apply(document); } catch (e) {}
        })();
        </script>
        """.replace("__DARK__", "1" if dark_mode else "0"),
        height=0,
        width=0,
    )
