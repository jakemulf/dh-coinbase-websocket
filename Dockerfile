FROM ghcr.io/deephaven/server:0.14.0
COPY requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt
