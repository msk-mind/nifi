# nifi

This repo contains:

- NiFi docker setup
- a Spark script that gets kicked off as one of our ETL pipelines.

## Getting Started

Start the nifi container with a clean configuration:
```
    make run
```
Stop the nifi container:

```
    make clean
```    


Start the nifi container with a persisting configuration:
```
    docker-compose up -d
``` 
Stop the nifi container:
```
    docker-compose down
```  
Once started, access the webapp at localhost:8083/nifi

---

## Additional NARs

To add or update nar libraries to the server, see [nifi_lib](./lib/)

---

## Configuration

Configuration files are in the [conf](./conf/) directory.

For connecting to HDFS, we need to copy configuration files from HDFS.

- hdfs-site.xml : downloaded from sandbox HDP && added dfs.client.use.datanode.hostname property
- core-site.xml : downloaded from sandbox HDP

Then add your HDP ip address mapping to the nifi container.
```
docker exec -u 0 nifi_ext /bin/sh -c "echo '<ip> sandbox-hdp.hortonworks.com' >> /etc/hosts"
```
*Note*: if sandbox-hdp is running in a different docker container, the ip address can be found by running:
```
docker inspect sandbox-hdp | grep Gateway
```
---

For JVM memory setting

- bootstrap.conf : Download from Sandbox HDP and modify the properties 
    ```
    java.arg.2=-Xms4g
    java.arg.3=-Xmx4g
    ```

---

For Atlas - NiFi integration

- atlas-application.properties : Download from Sandbox HDP and add the property below
    ```
    atlas.notification.hook.asynchronous=false
    ```
    
---

For Persisting the state and configuration

- the initalized conf folder must be copied into the volume 
    ```
    docker cp <container id>:/opt/nifi/nifi-current/conf ./volumes/
    ```

## Python scripts and tests

- **src**: scripts that get called from nifi pipelines.
- **test**: unit tests for the scripts in `src`.

To run tests, setup a virtual env.
```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

Then run the tests in the virtual env.
```
cd test
pytest
```

Use ```pytest -s -v``` for details.

## Reference:

Base nifi docker image docs - https://hub.docker.com/r/apache/nifi/
