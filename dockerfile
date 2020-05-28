FROM apache/nifi

USER root

COPY --chown=nifi:nifi lib/nifi-dicom-1.4.nar /opt/nifi/nifi-current/lib/
# conf files used by HdfsConnection.xml template
# (https://github.com/msk-mind/etl/blob/master/HdfsConnection.xml)
COPY --chown=nifi:nifi conf/core-site.xml /opt/nifi/nifi-current/conf/
COPY --chown=nifi:nifi conf/hdfs-site.xml /opt/nifi/nifi-current/conf/

# change permission for openshift
RUN chgrp -R 0 /opt/nifi/nifi-current && \
    chmod -R g=u /opt/nifi/nifi-current

USER nifi
