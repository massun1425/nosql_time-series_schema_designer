FROM ubuntu:18.04


ADD ./experiments /experiments
WORKDIR /experiments

RUN apt-get update
RUN apt-get install -y curl
RUN apt-get install -y nodejs vim mysql-client
RUN apt-get install -y npm g++ build-essential

WORKDIR /experiments/rubis
RUN npm install
