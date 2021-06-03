# SABD 2020/2021 Progetto 1
Marco Marcucci, Giuseppe Lasco

##Prerequisiti
* Installare Apache Spark
* Eseguire `export SPARK_HOME=<spark-path> `
* Installare Docker
* Inserire in */etc/host* `127.0.0.1	hbase`
* 

## Docker container descritption and initialization notes:

### Containers starting and stopping scripts:
* `start-docekrs.sh` creates a Docker network and starts every container,
* `stop-dockers.sh` stops and removes every container and deletes the network.

### First start notes:
* **in /etc/hosts**:

	1. 	insert `127.0.0.1	hbase`
	2. insert `127.0.0.1	influxdb`

* **NiFi**: 

	1. connect to _http://localhost:9880/nifi/_,
	2. press on "*upload template*" under the "*Operate*" box,
	3. select `SABD_project1_dataset_import.xml` file inside the `nifi_template` directory.

* **Grafana**:
	1. connect to _http://localhost:3000/_,
	2. login using username:`admin` and password:`password`
	3. select the "*setting*" panel,
	4. choose "*datasources*" and add a new datasource,
	5. choose influxDB as datasource, set _http://influxdb:8086_ as url, set `Queries` as database and `admin`, `password` as datastore credentials,
	6. select the "*+*" tab,
	7. choose "*import*" option,
	8. select all of queries dashboard inside the `grafana-dashboards/grafana-JSON` directory.

### Containers descriptions:
* **NiFi** container:

	Used to import datasets from github to HDFS.
	`nifi_conf` and `nifi_state` directories are bounded to the container to implement local host machine persistence.
	
	`nifi-hdfs` files are automatically uploaded to the container and used to allow NiFi and HDFS communication.
	
* **HDFS** containers:
	
	Four containers working one as Namenode and three as Datanodes for the Hadoop Distributed Filesystem.
	Script inside the `hdfs-script` directory are automatically uploaded to the container and executed to format and start the distributed filesystem and create directories to store input and output files.
	
*  **HBase** container:

	For HDFS output file export to NoSQL datastore feature.
	
* **Grafana** container:

	For queries results graphical representation. The `grafana_storage` folder is automatically bounded to the container to implement local host machine persistence.
	
	In `grafana-dashboards/images` there are graphics result updated to 26th of May, 2020 (18:00 UTC+1).
	
* **InfluxDB** container:

	For queries result export from HDFS to Grafana. The `influx_storage` directory is automatically bounded to the container to implement local host machine persistence of the time series database.
