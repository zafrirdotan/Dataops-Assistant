# Shared base image for DataOps pipelines
FROM python:3.10-slim
WORKDIR /app
# Copy the shared requirements file
COPY base.requirements.txt ./requirements.txt
# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt
# Optionally, add any common environment variables or tools here
# ENV PYTHONUNBUFFERED=1
# The base image does not set an entrypoint; pipeline images will override this
