# STREAMLIT DEMO UI

Streamlit app for viewing Databricks-backed product revenue and sales data.

## Run locally

1. Install dependencies:

```powershell
pip install -r requirements.txt
```

2. Configure secrets using one of these options:

- Option A: create `.env`
- Option B: create `.streamlit/secrets.toml`

The app now reads configuration in this order:

1. `st.secrets`
2. environment variables
3. `.env`

Required keys:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_WAREHOUSE_ID`

Optional keys:

- `DATABRICKS_SQL_POLL_SECONDS`
- `DATABRICKS_SQL_TIMEOUT_SECONDS`
- `DATABRICKS_SQL_WAIT_TIMEOUT`

Example `.streamlit/secrets.toml`:

```toml
DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
DATABRICKS_TOKEN = "your-token"
DATABRICKS_WAREHOUSE_ID = "your-warehouse-id"

DATABRICKS_SQL_POLL_SECONDS = "1"
DATABRICKS_SQL_TIMEOUT_SECONDS = "60"
DATABRICKS_SQL_WAIT_TIMEOUT = "10s"
```

3. Start the app:

```powershell
streamlit run app.py
```

## Run with Docker

1. Prepare environment variables:

```powershell
Copy-Item .env.example .env
```

2. Optional: prepare Streamlit secrets file if you prefer `st.secrets` over env vars:

```powershell
Copy-Item .streamlit\secrets.toml.example .streamlit\secrets.toml
```

3. Build and start the container:

```powershell
docker compose up --build
```

4. Open the app:

```text
http://localhost:8501
```

If you only want to use `docker run`:

```powershell
docker build -t coop-kobe:local .
docker run --rm -p 8501:8501 --env-file .env -v ${PWD}\.streamlit:/app/.streamlit:ro coop-kobe:local
```

Notes:

- The container runs the existing entrypoint: `streamlit run app.py`.
- Docker Compose uses the fixed project name `coop-kobe`.
- Docker Compose reads `.env` and mounts `.streamlit` as read-only for local secrets usage.
- The container includes a healthcheck against `http://127.0.0.1:8501/_stcore/health`.
- Do not put real credentials into `Dockerfile` or `docker-compose.yml`.

## Deploy Docker To Cloud Run

For a real container platform deployment, use the Cloud Run guide in:

- `deploy/cloud-run/README.md`
- `deploy/cloud-run/service.yaml`

The Docker image now respects the runtime `PORT` environment variable, so it can run both locally and on Cloud Run.

## Deploy safely

- Do not commit `.env`
- Do not commit `.streamlit/secrets.toml`
- Only commit `.streamlit/secrets.toml.example`
- Only commit `.env.example`
- In Streamlit Community Cloud or another deploy platform, add the same keys in the platform's Secrets or Environment Variables UI

To prepare secrets locally from the example:

```powershell
Copy-Item .streamlit\\secrets.toml.example .streamlit\\secrets.toml
```

Then replace the placeholder values with real credentials.
