FROM apache/flink:1.18.1-scala_2.12

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip unzip && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN mkdir -p /opt/flink/pyflink && \
    unzip /opt/flink/opt/python/pyflink.zip -d /opt/flink/pyflink

RUN pip install --no-cache-dir apache-beam protobuf

USER flink
