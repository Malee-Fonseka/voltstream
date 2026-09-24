# voltstream — app image: simulators + API (decision T048).
#
# §7.3: "the API image does not ship a 300 MB Spark distribution." This image installs
# only the `api` and `sim` optional-dependency groups — never `spark`. The
# `import pyspark` failure this produces on purpose is the proof the image split is
# real, not just a comment (see T048's Done-when).
#
# Multi-stage so the final image carries no build toolchain: the builder stage installs
# into a throwaway venv from a *non-editable* install of the package (the source tree is
# not needed at runtime), and only that venv is copied into the slim final stage.

FROM python:3.11-slim-bookworm AS builder

WORKDIR /build
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# Dependencies first, source second, so a source edit does not re-resolve and re-download
# the whole dependency tree. Same reasoning as spark.Dockerfile, where it matters far more.
COPY pyproject.toml README.md ./
RUN mkdir -p src/voltstream \
    && printf "__version__ = \"0.0.0\"\n" > src/voltstream/__init__.py \
    && pip install --no-cache-dir ".[api,sim]" \
    && pip uninstall -y voltstream

COPY src/ ./src/
RUN pip install --no-cache-dir --no-deps ".[api,sim]"

FROM python:3.11-slim-bookworm

RUN groupadd --system voltstream && useradd --system --gid voltstream --create-home voltstream

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# The dashboard is a single static file served by the API (T135). Copied rather than
# mounted so the image is self-contained: `docker run voltstream-app` serves the page
# without needing the repository on the host.
COPY dashboard/ /app/dashboard/
COPY config/ /app/config/

WORKDIR /app
USER voltstream

# No default CMD — this image serves several entry points (the console scripts
# pyproject.toml defines: voltstream-producer, voltstream-dropper, voltstream-api, ...);
# docker-compose.yml sets `command:` per service instead of relying on one default.
