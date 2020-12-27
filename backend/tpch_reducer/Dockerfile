FROM yusukew/ruby-2.6.5-ubuntu

ADD ./delete_cascade_rows.rb .

RUN apt-get update && apt-get install -y mysql-client libmysqlclient-dev
RUN gem install mysql2
