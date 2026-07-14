# Archived upstream QUEST setup

This file is retained only as a marker for the former upstream setup instructions. It is not the setup guide for the code in this repository.

The current integration no longer requires editing `db/connector/connector.py` with hard-coded credentials. It loads PostgreSQL/pgvector connection settings and Azure OpenAI settings from `.env`, and the repository provides `docker-compose.yml` plus `init_db.sql`.

Use [README.md](README.md) for the maintained environment, pgvector, smoke-test and root-orchestrator instructions.
