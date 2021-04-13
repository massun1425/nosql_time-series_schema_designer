FROM cassandra:3.11.8

RUN sed -i -e "s/batch_size_fail_threshold_in_kb: .*/batch_size_fail_threshold_in_kb: 100000/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/write_request_timeout_in_ms: .*/write_request_timeout_in_ms: 60000/g" /etc/cassandra/cassandra.yaml

RUN sed -i -e "s/\#-Xms4G/-Xms12G/g" /etc/cassandra/jvm.options
RUN sed -i -e "s/\#-Xmx4G/-Xmx12G/g" /etc/cassandra/jvm.options

WORKDIR /
