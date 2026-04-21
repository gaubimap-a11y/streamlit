# TMN COOP Kobe - Web Application

Hệ thống quản lý tích hợp Databricks SQL và giao diện Streamlit.

## 🛠 Tech Stack
-   **Frontend:** [Streamlit](https://streamlit.io/) (v1.40.0+)
-   **Database:** [Databricks SQL](https://www.databricks.com/product/databricks-sql) (Delta Lake)
-   **Connector:** `databricks-sql-connector`
-   **Validation:** Pydantic v2

## 🚀 Cài đặt & Khởi chạy

### 1. Cấu hình kết nối
Tạo file `webapp/databricks.local.cfg` (copy từ `databricks.cfg`) và điền thông tin:
```ini
[databricks]
server_hostname = "..."
http_path = "..."
access_token = "..."
```

### 2. Cài đặt thư viện
Dự án sử dụng [Poetry](https://python-poetry.org/) để quản lý môi trường:
```bash
cd webapp
poetry install
```

Hoặc kích hoạt virtual environment:
```bash
poetry shell
```

### 3. Cấu trúc Database & Migration & Seed data
Ứng dụng sử dụng migration files để quản lý schema của Databricks.

-   **Thư mục scripts:** `webapp/migrations/versions/`
-   **Bảng lịch sử:** `tmn_kobe.config.migration_history`
-   **Schema:** `config`, `master`, `transaction`, `auth`, `analytical`
    - `config`: Cấu hình hệ thống.
    - `master`: Dữ liệu master.
    - `transaction`: Dữ liệu giao dịch, phát sinh theo thời gian thực.
    - `auth`: Dữ liệu người dùng/phân quyền.
    - `analytical`: Dữ liệu phân tích, tổng hợp.
-   **Cách thêm migration mới:** 
    Tạo file `.sql` mới trong thư mục `versions/` theo mẫu `YYYYMMDD_NNN_description.sql`.
    - `YYYYMMDD`: Ngày tạo (Ví dụ: 20240410).
    - `NNN`: Số thứ tự tịnh tiến trong ngày (001, 002...).
    - `description`: Mô tả ngắn gọn nhiệm vụ bằng tiếng Anh, không dấu (VD: `initial_schema`).

Hệ thống tự động nạp dữ liệu mẫu (seeds) sau khi hoàn tất migration để phục vụ phát triển và kiểm thử.

-   **Thư mục seeds:** `webapp/migrations/seed_data/`
-   **Dữ liệu Auth (`auth/`):**
    -   **Users:** `admin`, `manager`, `user1`. (Mật khẩu mặc định: `password123`).
    -   **Roles:** `Admin`, `Manager`, `Staff`.
    -   **Permissions:** Cấu hình quyền truy cập dashboard, quản lý sản phẩm...
-   **Dữ liệu Master (`master/`):**
    -   **Products:** Danh mục sản phẩm mẫu.

**Chạy migration thủ công qua CLI (cho dev, chạy script ngoài hoặc CI/CD):**
```bash
python migrations/migrate.py
```

### 4. Chạy ứng dụng
Sau khi mọi thứ đã sẵn sàng:
```bash
streamlit run app.py
```

## 🧪 Testing
```bash
pytest                 # Chạy Unit Tests
pytest -m integration  # Chạy Integration Tests (yêu cầu kết nối Databricks)
```
