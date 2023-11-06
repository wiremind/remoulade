# Please run docker build from remoulade main directory

FROM python:3.10

WORKDIR /usr/src/app

COPY . .
RUN pip install -e .[all]
RUN pip install -e examples/composition

WORKDIR /usr/src/app/examples/composition
CMD composition_worker
