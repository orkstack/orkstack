# The Python event-source fixture (docs/EVENT_SOURCE_IMAGES.md): the standard
# library and nothing of OrkStack's. It runs as whatever non-root uid the
# Orker gives it.
FROM python:3.13-alpine@sha256:9ef1a599f863231c3d91e941389bfdc4a2d6070557243d54df1a4e57015b960e
COPY source.py /usr/local/bin/source.py
USER 65532:65532
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "-I", "/usr/local/bin/source.py"]
LABEL org.orkstack.service.contract='1'
LABEL org.orkstack.service.profile='event-source/1'
LABEL org.orkstack.service.interface='{"schemaVersion":1,"profile":"event-source","eventSource":{"delivery":"DELIVERY_CLASS_AT_LEAST_ONCE","maxConcurrency":1,"deadLetter":true,"extendsLeases":true}}'
