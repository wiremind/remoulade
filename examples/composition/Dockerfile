# Please run docker build from remoulade main directory

FROM python:3.10

COPY . .
RUN pip install -e .[all]
RUN pip install -e examples/composition

WORKDIR /examples/composition
CMD composition_worker
