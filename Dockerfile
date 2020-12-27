FROM yusukew/ruby-2.6.5-ubuntu
MAINTAINER Y-Wakuta

ENV PATH /root/.rbenv/shims:$PATH

RUN apt-get update -qq && \
apt-get install -qq \
build-essential \
coinor-libcbc3 \
coinor-libcbc-dev \
coinor-libcgl-dev \
coinor-libclp-dev \
coinor-libcoinutils-dev \
coinor-libosi-dev \
git \
vim \
graphviz \
libmagickwand-dev \
libmysqlclient-dev \
libpq-dev \
&& apt-get clean

RUN echo "deb http://downloads.apache.org/cassandra/debian 40x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list
RUN curl https://downloads.apache.org/cassandra/KEYS | apt-key add -
RUN apt-get install -y apt-transport-https
RUN apt-get update
RUN apt-get install -y cassandra

RUN apt-get install -y python-pip
RUN pip install six cassandra-driver

ADD . /nose-cli

WORKDIR /nose-cli
RUN rbenv global 2.6.5
RUN gem install bundler
RUN bundle install

