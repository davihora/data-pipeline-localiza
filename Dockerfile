FROM apache/airflow:2.9.3-python3.11

USER airflow

# Install Poetry
RUN pip install --no-cache-dir poetry==1.8.3

# Copy dependency manifest (and lock file if present)
COPY pyproject.toml poetry.lock* ./

# Install runtime deps into the active environment (no virtualenv inside container)
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi --without dev
