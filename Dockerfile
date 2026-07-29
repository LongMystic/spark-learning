FROM apache/spark:3.5.1

USER root

COPY environment/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /opt/spark-apps

USER spark