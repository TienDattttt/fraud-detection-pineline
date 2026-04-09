FROM apache/spark:3.5.3

USER root

# Install Python ML and streaming dependencies (NO Selenium/Chrome)
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    redis==5.0.0 \
    pyspark==3.5.3 \
    mlflow==2.12.0 \
    numpy==1.26.4

USER spark
