FROM prefecthq/prefect:3.9.4-python3.12
RUN pip install --no-cache-dir "psycopg[binary]==3.2.10" "requests==2.32.5"
WORKDIR /opt/flow
COPY etl.py flow.py ./
CMD ["python", "flow.py"]
