FROM yusukew/nose_environment

ADD . /nose-cli

WORKDIR /nose-cli
RUN rbenv global 2.6.5
RUN gem install bundler -v 2.4.22
RUN bundle install


