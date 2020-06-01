# nifi


Start nifi server
    
```
    make run
```    

Stop nifi server

```
    make clean
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

Reference:

Base nifi docker image docs - https://hub.docker.com/r/apache/nifi/
