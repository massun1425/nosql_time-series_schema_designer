FROM yusukew/nose_environment

ADD . /nose-cli

WORKDIR /nose-cli
RUN rbenv global 2.6.5
RUN gem install bundler
RUN bundle install

