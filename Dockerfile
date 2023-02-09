ARG PYTHON_TAG
FROM python:$PYTHON_TAG

USER root
WORKDIR /stg

### Must run as root: https://docs.github.com/en/actions/using-github-hosted-runners/about-github-hosted-runners#docker-container-filesystem
# RUN useradd -ms /bin/bash python && \
#   mkdir /src && \
#   chown -R python:python /src && \
#   chmod -R 777 /src

ARG REQUIREMENTS_FILE_PATH
COPY $REQUIREMENTS_FILE_PATH ./requirements.txt
RUN pip install -r requirements.txt

# USER python

WORKDIR /src

COPY ./pyflake_client ./pyflake_client

CMD ["bash"]
