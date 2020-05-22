FROM apache/nifi
LABEL maintainer="Arfath Pasha <pashaa@mskcc.org>"
LABEL maintainer="Doori Rose" <RoseD2@mskcc.org>

USER root

COPY --chown=nifi:nifi lib/nifi-dicom-1.4.nar /opt/nifi/nifi-current/lib/

USER nifi