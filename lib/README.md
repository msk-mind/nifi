# nifi_lib


Generate nar libraries that need to be added to nifi server. 

```
    make lib
```  

The generated libraries get added to this directory and get automatically picked up from this directory when nifi server is started. 
    
- hdfs-site.xml : downloaded from sandbox HDP & added dfs.client.use.datanode.hostname property
- core-site.xml : downloaded from sandbox HDP

