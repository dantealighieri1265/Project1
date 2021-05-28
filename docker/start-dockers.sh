#!/bin/bash
docker network create --driver bridge apache_network
docker run -p 9880:9880 -d --network=apache_network -v nifi_conf:/opt/nifi/nifi-current/conf -v nifi_state:/opt/nifi/nifi-current/state -e NIFI_WEB_HTTP_PORT='9880' --name=nifi apache/nifi
docker cp nifi-hdfs nifi:/opt/nifi/nifi-current
docker run -t -i --rm -h hbase-docker -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16020:16020 -p 16201:16201 -p 16301:16301 -d --network=apache_network --name=hbase-docker harisekhon/hbase:1.4
docker run -t -i --rm -h slave1-docker -p 9864:9864 -d --network=apache_network --name=slave1 effeerre/hadoop
docker run -t -i --rm -h slave2-docker -p 9863:9864 -d --network=apache_network --name=slave2 effeerre/hadoop
docker run -t -i --rm -h slave3-docker -p 9862:9864 -d --network=apache_network --name=slave3 effeerre/hadoop
docker run -t -i --rm -h master-docker -p 9870:9870 -p 9871:54310 -d --network=apache_network --name=master effeerre/hadoop
docker cp hdfs-script/start-hdfs.sh master:/start-hdfs.sh
docker exec -it master sh /start-hdfs.sh
#docker run -it --name=hbase -h hbase -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16020:16020 -p 16201:16201 -p 16301:16301 -d harisekhon/hbase:1.4
#docker run -d -p 8086:8086 -v influx_storage:/var/lib/influxdb -e INFLUXDB_USERNAME=admin -e INFLUXDB_PASSWORD=password --network=apache_network --name=influxdb influxdb
#docker run -d -p 3000:3000 -v grafana_storage:/var/lib/grafana -e GF_SECURITY_ADMIN_USER=admin -e GF_SECURITY_ADMIN_PASSWORD=password --network=apache_network --name=grafana grafana/grafana:6.5.0
