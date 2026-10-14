# Same Spark image/version upstream's dev/.env.example pins as its primary
# quartet (Spark 4.0.1, Scala 2.13, Java 17) -- reused rather than repinned,
# so capture-pyspark's py4j bridge is exercised against the same py4j the
# rest of this project already targets.
FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu

USER root
WORKDIR /opt/apex

# Installed from source, not PyPI -- these are this branch's own lanes.
COPY capture-pyspark/pyproject.toml ./capture-pyspark/pyproject.toml
COPY capture-pyspark/src ./capture-pyspark/src
COPY stream-kafka/pyproject.toml ./stream-kafka/pyproject.toml
COPY stream-kafka/src ./stream-kafka/src

RUN pip3 install --no-cache-dir -e ./capture-pyspark -e ./stream-kafka "kafka-python>=2.0,<3"

COPY dev-pyspark/jobs ./jobs

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["/opt/apex/jobs/skew_join_job.py"]
