# Please run docker build from remoulade main directory

FROM python:3.12

WORKDIR /usr/src/app

COPY . .
ARG VERSION=4
RUN SETUPTOOLS_SCM_PRETEND_VERSION_FOR_REMOULADE=${VERSION} pip install -e .[all]
RUN pip install -e examples/composition

WORKDIR /usr/src/app/examples/composition
CMD composition_worker
