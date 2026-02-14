FROM apache/flink:1.18.1-scala_2.12

USER root

# -----------------------------
# Install Python + system tools
# -----------------------------
RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-dev \
        build-essential && \
    ln -s /usr/bin/python3 /usr/bin/python

# -----------------------------
# Python dependencies
# MUST include apache-flink
# -----------------------------
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
        apache-flink==1.18.1 \
        apache-beam==2.48.0 \
        numpy==1.24.4 \
        scipy==1.10.1 \
        scikit-learn==1.2.2 \
        joblib==1.3.2 \
        cloudpickle \
        protobuf \
        py4j \
        crcmod

# -----------------------------
# Shared model mount
# -----------------------------
RUN mkdir -p /mnt/models

USER flink
