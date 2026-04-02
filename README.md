# STREAMLIT DEMO UI

Streamlit app for viewing Databricks-backed product revenue and sales data.

## Architecture

This app supports two deployment modes:

1. **Local Development** - Uses environment variables or Streamlit secrets for authentication
2. **Databricks Apps** - Uses automatic service principal authentication

The authentication flow is designed to work seamlessly in both environments:
- Local: Reads `DATABRICKS_TOKEN` from `.env` or `secrets.toml`
- Databricks Apps: Automatically uses app service principal credentials via Databricks SDK

## Run locally

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure secrets

Copy the example file and fill in your values:

```bash
cp .env.example .env
```

Required values in `.env`:
- `DATABRICKS_HOST` - Your workspace URL (e.g., `dbc-xxxxx.cloud.databricks.com`)
- `DATABRICKS_TOKEN` - Your personal access token
- `DATABRICKS_WAREHOUSE_ID` - SQL Warehouse ID to query

**How to get these values:**
- **Host**: Found in your workspace URL
- **Token**: Workspace Settings → Developer → Access Tokens → Generate New Token
- **Warehouse ID**: SQL Warehouses → Click your warehouse → Copy ID from URL or HTTP Path

**Alternative:** You can also use `.streamlit/secrets.toml` instead of `.env`:

```toml
DATABRICKS_HOST = "dbc-xxxxx.cloud.databricks.com"
DATABRICKS_TOKEN = "dapi..."
DATABRICKS_WAREHOUSE_ID = "..."
```

### 3. Start the app

```bash
streamlit run app.py
```

The app will be available at `http://localhost:8501`

## Deploy to Databricks Apps

This app is pre-configured for Databricks Apps deployment with `app.yaml`.

### Prerequisites

1. Unity Catalog table `workspace.default.gold_drink_sales` must exist with schema:
   - `product_name` (string)
   - `total_revenue` (decimal)
   - `total_sales` (bigint)

2. SQL Warehouse must be available in workspace

### Deploy using Databricks CLI

```bash
databricks apps deploy streamlit-git \
  --source-code-path /Workspace/Users/your-email/streamlit-git
```

### Deploy using Databricks SDK (Python)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import apps

w = WorkspaceClient()

# Create app
app_obj = apps.App(name="my-streamlit-app")
w.apps.create(app=app_obj).result()

# Deploy
deployment = apps.AppDeployment(
    source_code_path="/Workspace/Users/your-email/streamlit-git",
    mode=apps.AppDeploymentMode.SNAPSHOT
)
w.apps.deploy(app_name="my-streamlit-app", app_deployment=deployment).result()
```

### Grant Permissions

After deployment, grant permissions to the app's service principal:

```sql
-- Grant SELECT on table
GRANT SELECT ON TABLE workspace.default.gold_drink_sales 
TO `<service-principal-uuid>`;

-- Grant CAN_USE on SQL Warehouse (via UI or API)
```

**Note:** The app will automatically authenticate using its service principal. No manual token configuration needed!

## Run with Docker (Local)

### 1. Prepare environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 2. Build and run

```bash
docker compose up --build
```

### 3. Access app

Open `http://localhost:8501`

**Docker run alternative:**

```bash
docker build -t streamlit-demo:local .
docker run --rm -p 8501:8501 --env-file .env streamlit-demo:local
```

## Configuration Files

- `app.yaml` - Databricks Apps configuration
- `requirements.txt` - Python dependencies
- `.env.example` - Template for local environment variables
- `.streamlit/config.toml` - Streamlit production settings
- `config/datasets.json` - Dataset and table configuration

## Security Best Practices

⚠️ **Never commit secrets to git:**
- `.env` is in `.gitignore`
- `.streamlit/secrets.toml` is in `.gitignore`
- Only commit `.env.example` and `secrets.toml.example`

✅ **For production deployments:**
- Use Databricks Apps (automatic service principal auth)
- Or use platform-specific secret management (e.g., Cloud Run secrets)

## Project Structure

```
streamlit-git/
├── app.py                          # Main entry point
├── app.yaml                        # Databricks Apps config
├── requirements.txt                # Dependencies
├── .env.example                    # Local config template
├── config/
│   └── datasets.json               # Table configuration
├── src/
│   ├── __init__.py
│   ├── application/                # Business logic
│   ├── domain/                     # Models and validation
│   ├── infrastructure/             # Data access (SQL Warehouse)
│   └── ui/                         # Streamlit UI components
└── tests/                          # Unit tests
```

## Troubleshooting

### Local: "Missing Databricks configuration: DATABRICKS_TOKEN"
- Check `.env` file exists and has correct values
- Ensure `DATABRICKS_TOKEN` is set (not empty)
- Verify token hasn't expired

### Databricks Apps: "INSUFFICIENT_PERMISSIONS"
- Grant SELECT permission on table to app service principal
- Grant CAN_USE permission on SQL Warehouse
- Find service principal UUID: Apps UI → Your App → Service Principal

### SQL Warehouse not starting
- Ensure warehouse is not stopped/deleted
- Check you have CAN_USE permission on the warehouse
- First query may take ~30s to start serverless warehouse

## Development

### Run tests

```bash
pytest
```

### Code structure

The app follows clean architecture principles:
- **Domain**: Core business models (DemoRequest, DemoReport)
- **Application**: Business logic (generate_demo_report)
- **Infrastructure**: External dependencies (SQL Warehouse access)
- **UI**: Streamlit presentation layer

## License

[Your License Here]
