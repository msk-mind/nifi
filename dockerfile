FROM apache/nifi

USER root


## add libs
COPY lib/nifi-dicom-1.4.nar /opt/nifi/nifi-current/lib/
# nifi-atlas integration
COPY lib/nifi-atlas-nar-1.11.0.nar /opt/nifi/nifi-current/lib/


RUN chown -R nifi:nifi /opt/nifi/nifi-current/lib/


## add confs
# changing some config values in host file may not change values in container file
# because start script replaces some values before server launch
COPY conf/nifi.properties /opt/nifi/nifi-current/conf/
COPY conf/bootstrap.conf /opt/nifi/nifi-current/conf/

# nifi-atlas integration
COPY conf/atlas-application.properties /opt/nifi/nifi-current/conf/


# conf files used by HdfsConnection.xml template
# (https://github.com/msk-mind/etl/blob/master/HdfsConnection.xml)
COPY conf/core-site.xml /opt/nifi/nifi-current/conf/

COPY conf/hdfs-site.xml /opt/nifi/nifi-current/conf/


RUN chown -R nifi:nifi /opt/nifi/nifi-current/conf/

# change permission for openshift
RUN chgrp -R 0 /opt/nifi/nifi-current && \
    chmod -R g=u /opt/nifi/nifi-current

USER nifi

ENTRYPOINT ["../scripts/start.sh"]