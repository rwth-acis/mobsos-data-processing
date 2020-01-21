# MobSOS Data Processing

[![Build Status](https://travis-ci.org/rwth-acis/mobsos-data-processing.svg?branch=master)](https://travis-ci.org/rwth-acis/mobsos-data-processing) [![codecov](https://codecov.io/gh/rwth-acis/mobsos-data-processing/branch/master/graph/badge.svg)](https://codecov.io/gh/rwth-acis/mobsos-data-processing) [![Join the chat at https://gitter.im/rwth-acis/mobsos](https://badges.gitter.im/rwth-acis/mobsos.svg)](https://gitter.im/rwth-acis/mobsos?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This service is part of the MobSOS monitoring concept and processes incoming monitoring data.

## Local Deployment

### Database

Set up the database. You can find the sql file in the [etc](etc) folder.

After that, configure the [property](etc/i5.las2peer.services.mobsos.dataProcessing.MobSOSDataProcessingService.properties) file of the service and enter your database credentials.

### Build

Execute the following command on your shell:

```shell
ant all
```

### Start

To start the service, use one of the available start scripts:

Windows:

```shell
bin/start_network.bat
```

Unix/Mac:
```shell
bin/start_network.sh
```

Have a look at the [manual](../../wiki/Manual) if you want to know how to monitor a node/service.

## How to run using Docker

First build the image:
```bash
docker build . -t mobsos-data-processing
```

Then you can run the image like this:

```bash
docker run -e MYSQL_USER=myuser -e MYSQL_PASSWORD=mypasswd -p 9011:9011 mobsos-data-processing
```

Replace *myuser* and *mypasswd* with the username and password of a MySQL user with access to a database named *LAS2PEERMON*.
By default the database host is *mysql* and the port is *3306*.
The las2peer node is available via port 9011.
Other nodes can now bootstrap this node and start sending messages if started with the ```--observer``` flag.

In order to customize your setup you can set further environment variables.

### Node Launcher Variables

Set [las2peer node launcher options](https://github.com/rwth-acis/las2peer-Template-Project/wiki/L2pNodeLauncher-Commands#at-start-up) with these variables.
The las2peer port is fixed at *9011*.

| Variable           | Default    | Description                                                                                                                                  |
| ------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| BOOTSTRAP          | unset      | Set the --bootstrap option to bootrap with existing nodes. The container will wait for any bootstrap node to be available before continuing. |
| SERVICE_PASSPHRASE | Passphrase | Set the second argument in *startService('<service@version>', '<SERVICE_PASSPHRASE>')*.                                                      |
| SERVICE_EXTRA_ARGS | unset      | Set additional launcher arguments. Example: ```--observer``` to enable monitoring.                                                           |

### Service Variables

See the [property file](etc/i5.las2peer.services.mobsos.dataProcessing.MobSOSDataProcessingService.properties) for an overview of the settings.
The database type is fixed at mysql for now.

| Variable       | Default     |
| -------------- | ----------- |
| MYSQL_USER     | *mandatory* |
| MYSQL_PASSWORD | *mandatory* |
| MYSQL_HOST     | mysql       |
| MYSQL_PORT     | 3306        |
| HASH_REMARKS   | FALSE       |


### Other Variables

| Variable | Default | Description                                                                |
| -------- | ------- | -------------------------------------------------------------------------- |
| DEBUG    | unset   | Set to any value to get verbose output in the container entrypoint script. |

### Custom Node Startup

If the variables are not sufficient for your setup you can customize how the node is started via arguments after the image name.
In this example we start the node in interactive mode:
```bash
docker run -it -e MYSQL_USER=myuser -e MYSQL_PASSWORD=mypasswd activity-tracker startService\(\'de.rwth.dbis.acis.activitytracker.service.ActivityTrackerService@0.6.0\', \'Passphrase\'\) startWebConnector interactive
```
Inside the container arguments are placed right behind the launch node command:
```bash
java -cp lib/* i5.las2peer.tools.L2pNodeLauncher -s service -p ${LAS2PEER_PORT} <your args>
```

### Volumes

The following places should be persisted in volumes in productive scenarios:

| Path              | Description                            |
| ----------------- | -------------------------------------- |
| /src/node-storage | Pastry P2P storage.                    |
| /src/etc/startup  | Service agent key pair and passphrase. |
| /src/log          | Log files.                             |

*Do not forget to persist you database data*
