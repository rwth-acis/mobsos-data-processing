MobSOS Data-Processing
===========================================
[![Build Status](https://travis-ci.org/rwth-acis/mobsos-data-processing.svg?branch=master)](https://travis-ci.org/rwth-acis/mobsos-data-processing)

This service is part of the MobSOS monitoring concept and processes incoming monitoring data. 

Database
--------
Set up the database. You can find different sql files in the [bin](bin) folder:

| ID  | Type |
| ------------- | ------------- |
| 1  | [DB2](bin/create_database_DB2.sql)  |
| 2  | [MySQL](bin/create_database_MySQL.sql)  |

After that configure the [property](etc/i5.las2peer.services.mobsos.dataProcessing.MonitoringDataProcessingService.properties) file of the service and enter your database credentials.

```INI
databaseTypeInt = 2
databaseUser = exampleuser
databasePassword = examplepass
databaseName = exampledb
databaseHost = localhost
databasePort = 3306
```


Build
--------
Execute the following command on your shell:

```shell
ant all 
```

Start
--------

To start the data-processing service, use one of the available start scripts:

Windows:

```shell
bin/start_network.bat
```

Unix/Mac:
```shell
bin/start_network.sh
```

--------
Have a look at the [manual](../../wiki/Manual) if you want to know how to monitor a node/service.
