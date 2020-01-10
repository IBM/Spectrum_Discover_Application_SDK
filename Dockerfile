FROM centos:7.7.1908

ENV IS_DOCKER_CONTAINER True
ENV LC_ALL en_US.UTF-8

RUN yum install -y python3 python3-devel python3-setuptools python3-pip openssl-devel gcc make nfs-utils && \
    python3 -m pip install ibm_spectrum_discover_application_sdk && \
    yum clean all

ENTRYPOINT []
CMD []
