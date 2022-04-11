FROM cassandra:3.11.8

RUN sed -i -e "s/batch_size_fail_threshold_in_kb: .*/batch_size_fail_threshold_in_kb: 100000/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/write_request_timeout_in_ms: .*/write_request_timeout_in_ms: 100000/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/read_request_timeout_in_ms: .*/read_request_timeout_in_ms: 100000/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/request_timeout_in_ms: .*/request_timeout_in_ms: 100000/g" /etc/cassandra/cassandra.yaml

# <disable cache>
RUN sed -i -e "s/key_cache_size_in_mb:/key_cache_size_in_mb: 0/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/\# file_cache_size_in_mb:.*/file_cache_size_in_mb: 0/g" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/counter_cache_size_in_mb:/counter_cache_size_in_mb: 0/g" /etc/cassandra/cassandra.yaml
# </disable cache>


WORKDIR /
