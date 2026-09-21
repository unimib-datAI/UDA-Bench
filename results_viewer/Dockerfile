# Build dalla root del repo: docker compose -f results_viewer/docker-compose.yml up -d --build
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends unzip && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY results_viewer/requirements.txt results_viewer/
RUN pip install --no-cache-dir -r results_viewer/requirements.txt

# Dall'archivio servono solo valutazioni e risposte, non input e cache dei sistemi.
RUN --mount=type=bind,source=results_viewer/data/uda_outputs.zip,target=/tmp/uda_outputs.zip \
    unzip -q /tmp/uda_outputs.zip '*/evaluation/*' '*/evaluation_llm_quest/*' '*/csv/*' -d results_viewer/data

COPY Query/Finan Query/Finan
COPY note_valutazione.md ./
COPY results_viewer/*.py results_viewer/README.md results_viewer/

EXPOSE 8501
CMD ["streamlit", "run", "results_viewer/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
