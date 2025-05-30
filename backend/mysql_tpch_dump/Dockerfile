FROM mysql:8.0.33-debian

# MySQL APTリポジトリを無効化
RUN rm -f /etc/apt/sources.list.d/mysql.list

RUN apt-get update
RUN apt-get install -y unzip

COPY ./validate_join.sh ./
COPY ./tpch_sf_1.sql.zip ./
RUN unzip ./tpch_sf_1.sql.zip
