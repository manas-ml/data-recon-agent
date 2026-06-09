"""Configuration and environment loading for the reconciliation agent.

All settings are sourced from environment variables. Inside Databricks Apps the
Databricks host and token are auto-detected by the Databricks SDK, so they are
optional here and default to empty strings; the SDK's default credential
resolution (WorkspaceClient()) takes over when they are unset.

Never hardcode secrets in this file — values flow in from Databricks secrets via
app.yaml (ANTHROPIC_API_KEY, DATABRICKS_WAREHOUSE_ID) or the local .env during
development.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Strongly-typed application settings loaded from the environment."""

    # --- Anthropic / Claude ---------------------------------------------------
    anthropic_api_key: str = ""
    # The agent brain. Explicitly pinned per the demo specification.
    anthropic_model: str = "claude-sonnet-4-20250514"

    # --- Databricks -----------------------------------------------------------
    # Auto-detected by the Databricks SDK inside Databricks Apps. Left blank by
    # default so WorkspaceClient() falls back to native app credentials.
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_warehouse_id: str = ""

    # --- Unity Catalog target -------------------------------------------------
    catalog: str = "main"
    schema_name: str = "recon_demo"

    # --- MLflow ---------------------------------------------------------------
    mlflow_experiment: str = "/Shared/recon-agent"

    # --- Agent loop -----------------------------------------------------------
    max_agent_iterations: int = 10

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # Allow DATABRICKS_HOST etc. without a prefix.
        extra="ignore",
        case_sensitive=False,
    )

    @property
    def fq_audit_table(self) -> str:
        """Fully-qualified name of the audit log Delta table."""
        return f"{self.catalog}.{self.schema_name}.recon_audit_log"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance (read once per process)."""
    return Settings()
