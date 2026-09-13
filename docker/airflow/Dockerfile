# Airflow with this project's dependencies baked in.
#
# Without this, Airflow reinstalls pandas/boto3/pandera/etc. from PyPI every
# time the container starts (_PIP_ADDITIONAL_REQUIREMENTS), which is slow and
# means what's actually running can vary run to run. Baking it into the image
# instead is what makes the image both reproducible and something worth
# pushing to a registry.
FROM apache/airflow:2.10.2-python3.11

COPY requirements.txt /tmp/requirements.txt
# hadolint ignore=DL3013
RUN pip install --no-cache-dir -r /tmp/requirements.txt
