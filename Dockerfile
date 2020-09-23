FROM ubuntu:16.04
MAINTAINER Yusuke Wakuta

# Install packages for building ruby
RUN apt-get update
RUN apt-get install -y --force-yes build-essential curl git vim
RUN apt-get install -y --force-yes zlib1g-dev libssl-dev libreadline-dev libyaml-dev libxml2-dev libxslt-dev
RUN apt-get clean

# Install rbenv and ruby-build
RUN git clone https://github.com/sstephenson/rbenv.git /root/.rbenv
RUN git clone https://github.com/sstephenson/ruby-build.git /root/.rbenv/plugins/ruby-build
RUN ./root/.rbenv/plugins/ruby-build/install.sh
ENV PATH /root/.rbenv/bin:$PATH
RUN echo 'eval "$(rbenv init -)"' >> /etc/profile.d/rbenv.sh # or /etc/profile

# Install multiple versions of ruby
ENV CONFIGURE_OPTS --disable-install-doc
RUN xargs -L 1 rbenv install 2.6.0

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

ADD . /td_NoSE

WORKDIR /td_NoSE
RUN rbenv global 2.6.0
RUN gem install bundler
RUN bundle install

