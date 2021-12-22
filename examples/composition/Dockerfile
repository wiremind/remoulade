# Please run docker build from remoulade main directory

FROM python:3.10

COPY . .
RUN pip install -e .[all]
RUN pip install --no-cache-dir -r examples/composition/requirements.txt

WORKDIR /examples/composition
CMD remoulade example
