import os

path = 'webapp/pages/menu_admin.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Thay đổi logic trong _render_page
old_call = """        table_event = _render_catalog_table(menus=menus)
        if table_event:
            if table_event["action"] == "edit":
                pending_mode = "edit"
                pending_target_key = table_event["menu_key"]
            if table_event["action"] == "toggle_active":
                pending_mode = "toggle_active"
                pending_target_key = table_event["menu_key"]"""

new_call = """        # Chuyển sang hiển thị dạng Tree với Kéo thả
        tree_structure = _build_menu_tree_structure(menus)
        tree_event = _render_menu_node_recursive(
            nodes=tree_structure, 
            actor_permissions=actor_permissions
        )
        
        if tree_event:
            if tree_event["action"] == "edit":
                pending_mode = "edit"
                pending_target_key = tree_event["menu_key"]
            if tree_event["action"] == "toggle_active":
                pending_mode = "toggle_active"
                pending_target_key = tree_event["menu_key"]"""

if old_call in content:
    content = content.replace(old_call, new_call)

# 2. Xóa function cũ _render_catalog_table (Tìm điểm bắt đầu và kết thúc của function)
def remove_function(file_content, func_name):
    lines = file_content.split('\n')
    start_line = -1
    for i, line in enumerate(lines):
        if line.startswith(f'def {func_name}'):
            start_line = i
            break
    
    if start_line == -1: return file_content
    
    # Tìm điểm kết thúc (ngắt indentation)
    end_line = start_line + 1
    while end_line < len(lines):
        if lines[end_line].strip() and not lines[end_line].startswith('    ') and not lines[end_line].startswith(') ->'):
            # Kiểm tra xem có phải tiếp tục signature không
            if '->' in lines[end_line-1] or lines[end_line-1].strip().endswith(':'):
                 pass
            else:
                 break
        end_line += 1
    
    del lines[start_line:end_line]
    return '\n'.join(lines)

# Xóa các hàm cũ không còn dùng
content = remove_function(content, '_render_catalog_table')
content = remove_function(content, '_normalize_search_text') # search text không còn dùng trong tree view này

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
