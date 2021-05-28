#!/bin/bash
docker kill master nifi slave1 slave2 slave3 hbase
docker rm nifi master slave1 slave2 slave3 hbase
docker network rm apache_network
