FROM registry.access.redhat.com/ubi8/s2i-core

ENV IS_DOCKER_CONTAINER True
ENV LC_ALL en_US.UTF-8

ARG major=0
ARG minor=0
ARG patch=22

RUN tar -C /install_media/ -xzf /install_media/ubi8_packages.tar.gz && \
    cp /install_media/ubi8_packages/ubi8_local.repo /etc/yum.repos.d/ && \
    yum install -y python39 python39-devel openssl-devel gcc make nfs-utils cifs-utils && \
    python3 -m pip install --upgrade --no-cache-dir pip==21.3 && \
    python3 -m pip install /install_media/ibm_spectrum_discover_application_sdk-${major}.${minor}.${patch}-py2.py3-none-any.whl && \
    yum clean all && \
    rm -f /etc/yum.repos.d/ubi8_local.repo

ENTRYPOINT []
CMD []
