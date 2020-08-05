# nifi


Start nifi server with a clean configuration
    
```
    make run
```    
Start nifi server with a presisting configuration
    
```
    docker-compose up -d
``` 

Stop nifi server

```
    make clean
```    
or

```
    docker-compose down
```  
---

Once started, open webapp in browser at localhost:8083/nifi

Upload your template from Operator box on left

Then drag and drop new template into the main canvas using Template icon

---

*To add or update nar libraries to the server, see [nifi_lib](./lib/)*

---

For connecting to HDFS

- hdfs-site.xml : downloaded from sandbox HDP && added dfs.client.use.datanode.hostname property
- core-site.xml : downloaded from sandbox HDP
- add your HDP ip to nifi vm.
```
docker exec -u 0 nifi_ext /bin/sh -c "echo '<ip> sandbox-hdp.hortonworks.com' >> /etc/hosts"
```
*Note*: if sandbox-hdp is running in a different docker, ip can be found by running:
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

For Presisting the state and configuration

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
