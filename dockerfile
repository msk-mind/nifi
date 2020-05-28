FROM apache/nifi

USER root

COPY --chown=nifi:nifi lib/nifi-dicom-1.4.nar /opt/nifi/nifi-current/lib/

# changing some config values in host file may not change values in container file
# because start script replaces some values before server launch
COPY --chown=nifi:nifi conf/nifi.properties /opt/nifi/nifi-current/conf/

# conf files used by HdfsConnection.xml template
# (https://github.com/msk-mind/etl/blob/master/HdfsConnection.xml)
COPY --chown=nifi:nifi conf/core-site.xml /opt/nifi/nifi-current/conf/
COPY --chown=nifi:nifi conf/hdfs-site.xml /opt/nifi/nifi-current/conf/

# change permission for openshift
RUN chgrp -R 0 /opt/nifi/nifi-current && \
    chmod -R g=u /opt/nifi/nifi-current

USER nifi

ENTRYPOINT ["../scripts/start.sh"]