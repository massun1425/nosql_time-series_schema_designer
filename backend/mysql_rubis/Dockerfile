FROM mysql:5.7

#参考 https://obel.hatenablog.jp/entry/20170219/1487486892
RUN echo "connect_timeout=5000" >> /etc/mysql/mysql.conf.d/mysqld.cnf
RUN echo "net_read_timeout=5000" >> /etc/mysql/mysql.conf.d/mysqld.cnf
RUN echo "net_write_timeout=5000" >> /etc/mysql/mysql.conf.d/mysqld.cnf

ADD ./experiments /experiments
ADD ./docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d
ADD ./additional_index.sql /
WORKDIR /experiments

WORKDIR /experiments/rubis
