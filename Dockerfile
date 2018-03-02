# use minimal alpine base image as we only need python and nothing else here
FROM python:3-alpine3.6

COPY src/dataagent.py /opt/dataagent.py

VOLUME /opt/ml/input /opt/ml/model /src-data

ENTRYPOINT ["python3.6", "-u", "/opt/dataagent.py"]
