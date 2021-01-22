FROM cassandra:3.11.8

RUN sed -i -e "s/batch_size_fail_threshold_in_kb: .*/batch_size_fail_threshold_in_kb: 100000/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/write_request_timeout_in_ms: .*/write_request_timeout_in_ms: 60000/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/\# memtable_offheap_space_in_mb: .*/memtable_offheap_space_in_mb: 2048/g" /etc/cassandra/cassandra.yaml

RUN sed -i -e "s/\#-Xms4G/-Xms150G/g" /etc/cassandra/jvm.options
RUN sed -i -e "s/\#-Xmx4G/-Xmx150G/g" /etc/cassandra/jvm.options

WORKDIR /tools
COPY ./create_keyspace.sh .
COPY ./create_keyspace_rubis.sh .
