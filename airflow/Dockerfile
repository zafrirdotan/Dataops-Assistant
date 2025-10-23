# This Dockerfile sets up Airflow with the requirements
FROM apache/airflow:3.1.0
# Install any python requirements as root, then switch back to airflow user
USER root
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && rm -f /requirements.txt
USER airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
