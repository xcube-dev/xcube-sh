ARG XCUBE_VERSION=0.4.2

FROM quay.io/bcdev/xcube:${XCUBE_VERSION}

ARG XCUBE_VERSION=0.4.2
ARG XCUBE_USER_NAME=xcube

LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name="xcube sh"
LABEL xcube_version=${XCUBE_VERSION}

SHELL ["/bin/bash", "-c"]

USER ${XCUBE_USER_NAME}

WORKDIR /home/${XCUBE_USER_NAME}
ADD --chown=1000:1000 environment.yml environment.yml
RUN mamba env   update -n xcube

ADD --chown=1000:1000 ./ .
RUN source activate xcube && python setup.py develop

ENTRYPOINT ["/bin/bash", "-c"]