FROM python:3.8

WORKDIR /data/refiner
COPY ./ ./
RUN pip install .

