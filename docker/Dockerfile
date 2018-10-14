FROM python:3.4
MAINTAINER Uemit Seren <uemit.seren@gmail.com>

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y libopenblas-dev libhdf5-dev

COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

RUN mkdir -p /usr/src/app/data

VOLUME  ["/usr/src/app/data"]

EXPOSE 8000

ENTRYPOINT ["gunicorn","app"]


