FROM quay.io/bcdev/xcube-python-deps:0.4.2

ARG XCUBE_SH_VERSION=latest
ARG XCUBE_USER_NAME=xcube

LABEL version=${XCUBE_SH_VERSION}
LABEL name=xcube-sh
LABEL maintainer=helge.dzierzon@brockmann-consult.de

SHELL ["/bin/bash", "-c"]

USER ${XCUBE_USER_NAME}
RUN whoami

RUN mkdir /home/${XCUBE_USER_NAME}/xcube-sh
WORKDIR /home/${XCUBE_USER_NAME}/xcube-sh
ADD --chown=1000:1000 environment.yml environment.yml
RUN conda env update -n xcube

ADD --chown=1000:1000 ./ .
RUN source activate xcube && python setup.py install

#RUN git clone https://github.com/dcs4cop/xcube-sh /home/${XCUBE_USER_NAME}/xcube-sh

ENTRYPOINT ["/bin/bash", "-c"]