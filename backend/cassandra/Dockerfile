FROM cassandra:3.11.8

#RUN apt-get update
#RUN apt-get install -y vim
COPY ./config/cassandra-env.sh /opt/cassandra/conf/
COPY ./config/jmxremote.password /etc/cassandra/
COPY ./config/jmxremote.access /opt/java/openjdk/lib/management/

RUN chown cassandra:cassandra /etc/cassandra/jmxremote.password
RUN chown 400 /etc/cassandra/jmxremote.password
USER cassandra

RUN sed -i -e "s/batch_size_fail_threshold_in_kb: 50/batch_size_fail_threshold_in_kb: 100000/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/write_request_timeout_in_ms: 20000/write_request_timeout_in_ms: 60000/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/\#-Xms4G/-Xms30G/g" /etc/cassandra/jvm.options
RUN sed -i -e "s/\#-Xmx4G/-Xmx30G/g" /etc/cassandra/jvm.options

WORKDIR /tools
COPY ./create_keyspace.sh .
COPY ./create_keyspace_rubis.sh .
