FROM python:3.12.12-slim
RUN pip install --no-cache-dir "psycopg[binary]==3.2.10" "requests==2.32.5"
WORKDIR /app
COPY etl.py run-all.sh ./
CMD ["/app/run-all.sh"]
