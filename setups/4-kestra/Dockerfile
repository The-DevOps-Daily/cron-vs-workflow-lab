FROM kestra/kestra:v2.0.10
USER root
RUN /app/.venv/bin/python3 -m pip install --no-cache-dir "psycopg[binary]==3.2.10" "requests==2.32.5"
COPY etl.py /opt/etl/etl.py
USER kestra
