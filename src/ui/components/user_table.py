from __future__ import annotations

import pandas as pd
import streamlit as st

from src.domain.user import UserRow


def render_user_table(
    rows: list[UserRow],
    *,
    key: str = "users_table_editor",
) -> tuple[str, str] | None:
    if not rows:
        return None

    dataframe = pd.DataFrame(
        [
            {
                "user_id": row.user_id,
                "username": row.username,
                "email": f"mailto:{row.email}",
                "is_active": row.is_active,
                "edit_action": False,
                "delete_action": False,
            }
            for row in rows
        ]
    )

    edited = st.data_editor(
        dataframe,
        use_container_width=True,
        hide_index=True,
        key=key,
        disabled=["user_id", "username", "email", "is_active"],
        column_config={
            "user_id": st.column_config.TextColumn("User ID", width="medium"),
            "username": st.column_config.TextColumn("Username", width="medium"),
            "email": st.column_config.LinkColumn(
                "Email",
                width="large",
                display_text="mailto:(.*)",
            ),
            "is_active": st.column_config.CheckboxColumn("Active"),
            "edit_action": st.column_config.CheckboxColumn("Sửa", width=1),
            "delete_action": st.column_config.CheckboxColumn("Xóa", width=1),
        },
    )

    if not isinstance(edited, pd.DataFrame) or edited.empty:
        return None

    edit_rows = edited.loc[edited["edit_action"] == True, "user_id"]  # noqa: E712
    if not edit_rows.empty:
        return ("edit", str(edit_rows.iloc[0]))

    delete_rows = edited.loc[edited["delete_action"] == True, "user_id"]  # noqa: E712
    if not delete_rows.empty:
        return ("delete", str(delete_rows.iloc[0]))

    return None
