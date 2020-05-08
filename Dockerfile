FROM continuumio/miniconda3

LABEL version=0.3.0.dev1
ARG XCUBE_VERSION=0.3.0.dev1
ARG XCUBE_USER_NAME=xcube

LABEL name=xcube-sh
LABEL maintainer=helge.dzierzon@brockmann-consult.de

SHELL ["/bin/bash", "-c"]

USER root
RUN whoami
RUN apt-get -y update && apt-get -y install vim

SHELL ["/bin/bash", "-c"]
RUN groupadd -g 1000 ${XCUBE_USER_NAME}
RUN useradd -u 1000 -g 1000 -ms /bin/bash ${XCUBE_USER_NAME}
RUN mkdir /workspace && chown ${XCUBE_USER_NAME}.${XCUBE_USER_NAME} /workspace
RUN chown -R ${XCUBE_USER_NAME}.${XCUBE_USER_NAME} /opt/conda

USER ${XCUBE_USER_NAME}
RUN whoami

RUN echo "conda activate xcube" >> ~/.bashrc


RUN git clone https://github.com/dcs4cop/xcube /home/${XCUBE_USER_NAME}/xcube
WORKDIR /home/${XCUBE_USER_NAME}/xcube
RUN conda env create
RUN source activate xcube && python setup.py install

RUN source activate xcube && conda install -c conda-forge oauthlib
RUN source activate xcube && python setup.py install
RUN source activate xcube && pip install requests_oauthlib

RUN git clone https://github.com/dcs4cop/xcube-sh /home/${XCUBE_USER_NAME}/xcube-sh
WORKDIR /home/${XCUBE_USER_NAME}/xcube-sh
RUN source activate xcube && python setup.py install


ENTRYPOINT ["/bin/bash", "-c"]