# Airflow with this project's dependencies baked in.
#
# Without this, Airflow reinstalls pandas/boto3/pandera/etc. from PyPI every
# time the container starts (_PIP_ADDITIONAL_REQUIREMENTS), which is slow and
# means what's actually running can vary run to run. Baking it into the image
# instead is what makes the image both reproducible and something worth
# pushing to a registry.
FROM apache/airflow:2.10.2-python3.11

COPY requirements.txt /tmp/requirements.txt
# Airflow's own constraints file pins the versions ITS code depends on
# (sqlalchemy, psycopg2-binary, pandas, ...). Applying it here means pip
# refuses to silently upgrade one of those out from under Airflow's own ORM
# instead of quietly breaking the scheduler at runtime - it would fail this
# build loudly instead, which is what requirements.txt's matching pins above
# are for.
# hadolint ignore=DL3013
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.11.txt" \
      -r /tmp/requirements.txt
