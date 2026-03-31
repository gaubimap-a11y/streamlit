# Deploy Guide - Google Cloud Run

## Mục tiêu
- Dùng Docker image hiện tại của dự án để deploy lên một nền tảng chạy container thật.
- Ưu tiên mô hình ít vận hành: build image, đẩy lên Artifact Registry, rồi deploy lên Cloud Run.

## Vì sao chọn Cloud Run
- Cloud Run deploy trực tiếp từ container image.
- Cloud Run hỗ trợ khai báo service bằng YAML và deploy bằng `gcloud run services replace`.
- Cloud Run truyền `PORT` runtime vào container; image của repo này đã được cập nhật để Streamlit nghe theo `PORT`.

## Files liên quan
- Manifest service: `deploy/cloud-run/service.yaml`
- Env mẫu cho non-secret config: `deploy/cloud-run/env.example`
- Docker image local: `Dockerfile`

## Chuẩn bị
1. Cài và đăng nhập Google Cloud CLI.
2. Chọn project và region:

```powershell
gcloud config set project PROJECT_ID
gcloud config set run/region REGION
```

3. Bật API cần thiết:

```powershell
gcloud services enable run.googleapis.com artifactregistry.googleapis.com secretmanager.googleapis.com
```

4. Tạo Artifact Registry repository:

```powershell
gcloud artifacts repositories create REPOSITORY --repository-format=docker --location=REGION
```

## Build và push image
1. Cấu hình Docker auth cho Artifact Registry:

```powershell
gcloud auth configure-docker REGION-docker.pkg.dev
```

2. Build image:

```powershell
docker build -t REGION-docker.pkg.dev/PROJECT_ID/REPOSITORY/coop-kobe:latest .
```

3. Push image:

```powershell
docker push REGION-docker.pkg.dev/PROJECT_ID/REPOSITORY/coop-kobe:latest
```

## Tạo secrets
Google khuyến nghị dùng Secret Manager cho thông tin nhạy cảm thay vì env vars thường.

Tạo các secret sau:
- `databricks-host`
- `databricks-token`
- `databricks-warehouse-id`

Ví dụ:

```powershell
echo -n "https://your-workspace.cloud.databricks.com" | gcloud secrets create databricks-host --data-file=-
echo -n "your-token" | gcloud secrets create databricks-token --data-file=-
echo -n "your-warehouse-id" | gcloud secrets create databricks-warehouse-id --data-file=-
```

Nếu secret đã tồn tại, dùng `gcloud secrets versions add SECRET_NAME --data-file=-`.

## Cập nhật manifest
Sửa `deploy/cloud-run/service.yaml`:
- đổi `metadata.name` nếu muốn
- thay `REGION`, `PROJECT_ID`, `REPOSITORY` trong `image`

Manifest hiện đã có:
- `containerPort: 8080`
- `startupProbe` gọi `/_stcore/health`
- giới hạn tài nguyên cơ bản

## Deploy service
1. Tạo hoặc replace service từ YAML:

```powershell
gcloud run services replace deploy/cloud-run/service.yaml --region REGION
```

2. Gắn secret vào service dưới dạng biến môi trường:

```powershell
gcloud run services update coop-kobe-ui `
  --region REGION `
  --update-secrets DATABRICKS_HOST=databricks-host:latest,DATABRICKS_TOKEN=databricks-token:latest,DATABRICKS_WAREHOUSE_ID=databricks-warehouse-id:latest
```

3. Nếu muốn bổ sung non-secret env vars từ file, có thể dùng:

```powershell
gcloud run services update coop-kobe-ui --region REGION --update-env-vars-file deploy/cloud-run/env.example
```

## Kiểm tra sau deploy
1. Lấy URL service:

```powershell
gcloud run services describe coop-kobe-ui --region REGION --format="value(status.url)"
```

2. Kiểm tra revision và trạng thái:

```powershell
gcloud run services describe coop-kobe-ui --region REGION
```

3. Xem logs:

```powershell
gcloud run services logs read coop-kobe-ui --region REGION
```

## Ghi chú vận hành
- App này là HTTP UI, phù hợp với Cloud Run service.
- Không mount `.streamlit/secrets.toml` trên Cloud Run; dùng Secret Manager sẽ phù hợp hơn.
- Nếu người dùng truy cập công khai, cần cân nhắc IAM/ingress theo yêu cầu bảo mật của dự án.
