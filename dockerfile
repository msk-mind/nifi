FROM apache/nifi

USER root

COPY --chown=nifi:nifi lib/nifi-dicom-1.4.nar /opt/nifi/nifi-current/lib/
COPY --chown=nifi:nifi lib/core-site.xml /opt/nifi/nifi-current/lib/
COPY --chown=nifi:nifi lib/hdfs-site.xml /opt/nifi/nifi-current/lib/

RUN chmod -R 666 /opt/nifi/nifi-current/

USER nifi
