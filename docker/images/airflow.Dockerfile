# Airflow image — orchestration dependencies and nothing else (T119, D6, §5.6).
#
# The point of a separate image is isolation, not convenience. Airflow pins a large and
# notoriously tight dependency set; PySpark pins another; FastAPI a third. Putting them in
# one image means every upgrade is a three-way constraint solve, and the usual outcome is
# that Airflow's pins quietly win and something else runs on a version it was never tested
# against.
#
# So this image has **no PySpark, no FastAPI and not even the `voltstream` package**. The
# DAGs do not import project code: they submit containers and check SQL results. The three
# tunables they need are read from `config/base.yaml`, mounted read-only, with
# `yaml.safe_load` — configuration, not a code dependency.
#
# Spark jobs run in `voltstream-spark:local`, launched through a Docker socket proxy. That
# is what keeps the two dependency sets apart while still letting one schedule the other.

ARG AIRFLOW_VERSION=3.0.3
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER airflow

# Exactly the three providers D6 names, installed against the matching constraints file.
# Without constraints, pip resolves the providers' own dependency ranges freely and can
# lift a core Airflow dependency to a version this Airflow was not released against —
# which fails at scheduler start, not at build time.
#
#   docker   — DockerOperator, to run the Spark job in its own container
#   amazon   — S3KeySensor and the S3 hook, pointed at MinIO
#   postgres — SQLCheckOperator / SQLValueCheckOperator against the serving database
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow-providers-docker" \
    "apache-airflow-providers-amazon" \
    "apache-airflow-providers-postgres"

# Guard the isolation claim at build time rather than trusting it. If a provider ever
# pulls PySpark in transitively, or someone adds the project package "just for the
# constants", the build fails here instead of the image silently becoming what §5.6 says
# it must not be.
RUN if python -c "import pyspark" 2>/dev/null; then \
      echo "pyspark must not be present in the Airflow image (see D6/§5.6)"; exit 1; \
    fi; \
    if python -c "import voltstream" 2>/dev/null; then \
      echo "the voltstream package must not be present in the Airflow image"; exit 1; \
    fi; \
    python -c "import airflow.providers.docker, airflow.providers.amazon, airflow.providers.postgres"
