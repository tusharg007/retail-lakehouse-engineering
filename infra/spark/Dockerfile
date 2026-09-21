FROM apache/spark:3.5.3-python3

USER root
RUN curl --fail --location --retry 3 --output /opt/spark/jars/delta-spark_2.12-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
 && curl --fail --location --retry 3 --output /opt/spark/jars/delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
USER spark
