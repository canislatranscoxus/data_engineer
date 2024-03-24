This folder contains a yaml file to start with Apache Iceberg and Spark in a local environment.


How to start our environment

this will create a Nessie catalog server
```
docker compose up catalog 
```

this will create a minio server accessible on localhost:9000
```
docker compose up storage
``` 

this will start the minio client which will create our initial buckets
```
docker compose up mc 
```

open container with pyspark notebook at localhost:8080
```
docker compose up spark-iceberg
```


this will open up a flink instance acting as a job manager accessible on localhost:8081.
```
docker compose up flink-jobmanager 
```

this will open up a flink instance acting as a task manager
```
docker compose up flink-taskmanager 
```

will create an instance of dremio accessible at localhost:9047
```
docker compose up dremio 
```

How to stop our environment
```
docker-compose down
```

To stop one container
```
docker-compose down <container_name>
```

To list all the containers
```
docker container ls
```

When a port is already used by an old container, stop and remove the old container.
To remove one container. 
```
docker rm -f <container-name>
```

To see used ports
```
sudo lsof -i -P -n | grep LISTEN
```


links:
https://github.com/developer-advocacy-dremio/definitive-guide-to-apache-iceberg/blob/main/Resources/Chapter_6/developer_env.md

