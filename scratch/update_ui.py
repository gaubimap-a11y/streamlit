import os

def insert_after(content, target, snippet):
    if target in content:
        return content.replace(target, target + snippet)
    return content

path = 'webapp/pages/menu_admin.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Sử dụng ngoặc kép cho chuỗi ngoài cùng để tránh xung đột với ngoặc đơn của code bên trong
new_functions = """
def _build_menu_tree_structure(menus: list[dict[str, object]]) -> list[dict[str, object]]:
    \"\"\"Chuyển danh sách phẳng sang cấu trúc cây.\"\"\"
    node_map = {str(m['menu_key']).lower(): {**m, 'children': []} for m in menus}
    root_nodes = []
    
    # Lấy danh sách keys đã được sắp xếp theo sort_order và key để đảm bảo thứ tự ban đầu
    sorted_keys = sorted(node_map.keys(), key=lambda k: (int(node_map[k].get('sort_order', 0)), k))
    
    for key in sorted_keys:
        node = node_map[key]
        parent = str(node.get('parent_key') or '').lower()
        if parent and parent in node_map:
            node_map[parent]['children'].append(node)
        else:
            root_nodes.append(node)
    
    # Sắp xếp lại con của từng node
    for node in node_map.values():
        node['children'].sort(key=lambda x: (int(x.get('sort_order', 0)), str(x.get('title')).lower()))
        
    return root_nodes

def _render_menu_node_recursive(
    *,
    nodes: list[dict[str, object]],
    actor_permissions: tuple[str, ...],
    level: int = 0
) -> dict[str, str] | None:
    \"\"\"Render các node menu theo đệ quy với tính năng kéo thả.\"\"\"
    if not nodes:
        return None

    event = None
    
    # Chuẩn bị danh sách cho sort_items
    node_labels = [f"[{n['menu_key']}] {n['title']}" for n in nodes]
    
    label_prefix = "    " * level
    msg_prefix = "🏠 Root" if level == 0 else "📂 Sub-menu"
    container_label = f"{label_prefix}{msg_prefix} (Kéo thả để sắp xếp lại)"
    
    # Hiển thị sort_items
    sorted_labels = sort_items(node_labels, label=container_label, direction='vertical')
    
    # Kiểm tra xem thứ tự có thay đổi không
    if sorted_labels != node_labels:
        new_ordered_keys = []
        for label in sorted_labels:
            try:
                k = label[1:label.find(']')]
                new_ordered_keys.append(k)
            except: pass
        
        if st.button(f\"💾 Lưu thứ tự mới\", key=f\"save_order_{level}_{nodes[0].get('parent_key')}\"):
            ok, reason = bulk_update_menu_order(ordered_keys=new_ordered_keys, actor_permissions=actor_permissions)
            if ok:
                st.success(\"Đã cập nhật thứ tự mới.\")
                st.rerun()
            else:
                st.error(f\"Lỗi: {reason}\")

    # Hiển thị chi tiết và các nút điều khiển cho từng node
    for node in nodes:
        indent_px = level * 30
        menu_key = node['menu_key']
        is_active = bool(node.get('is_active', True))
        
        # Header cấp menu
        st.markdown(f'''
            <div style=\"margin-left: {indent_px}px; border-left: 3px solid #007bff; padding-left: 15px; margin-top: 15px; margin-bottom: 5px;\">
                <span style=\"font-weight: bold; font-size: 1.1em;\">{node['title']}</span> 
                <span style=\"color: #6c757d; font-size: 0.85em; font-family: monospace;\">[{menu_key}]</span>
            </div>
        ''', unsafe_allow_html=True)
        
        # Row action
        act_col1, act_col2 = st.columns([1, 1])
        with act_col1:
            st.markdown(f'<div style=\"margin-left: {indent_px+15}px; color: #495057; font-size: 0.9em;\">'
                        f'📍 {node.get(\"route\") or \"Group Only\"} | 🛡️ {node.get(\"permission_code\")}</div>', 
                        unsafe_allow_html=True)
        with act_col2:
            btn_cols = st.columns([2, 2, 6])
            with btn_cols[0]:
                if st.button(\"✏️\", key=f\"btn_edit_{menu_key}\", help=\"Chỉnh sửa menu\"):
                    event = {\"action\": \"edit\", \"menu_key\": menu_key}
            with btn_cols[1]:
                toggle_icon = \"🛑\" if is_active else \"↩️\"
                toggle_help = \"Ngừng sử dụng\" if is_active else \"Tiếp tục sử dụng\"
                if st.button(toggle_icon, key=f\"btn_toggle_{menu_key}\", help=toggle_help):
                    event = {\"action\": \"toggle_active\", \"menu_key\": menu_key}
        
        # Nếu có con, render tiếp đệ quy trong expander
        if node['children']:
            with st.expander(f\"&nbsp;&nbsp;&nbsp;📂 Xem {len(node['children'])} menu con\", expanded=False):
                child_event = _render_menu_node_recursive(
                    nodes=node['children'], 
                    actor_permissions=actor_permissions, 
                    level=level + 1
                )
                if child_event:
                    event = child_event
                    
    return event
"""

content = insert_after(content, '_LOGIN_PAGE = "pages/login.py"', new_functions)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
