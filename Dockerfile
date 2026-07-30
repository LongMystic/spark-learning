FROM apache/spark:3.5.1

USER root

# install necessary library
COPY environment/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER spark