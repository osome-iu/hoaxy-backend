FROM continuumio/miniconda
# FROM frolvlad/alpine-miniconda3
# FROM frolvlad/alpine-miniconda2

LABEL maintainer="Phil Pope <p.e.pope01@gmail.com>"

#Install dependencies for hoaxy
RUN conda install -y \
        docopt \
        Flask \
        gunicorn \
        networkx \
        pandas \
        "psycopg2<2.7" \
        python-dateutil \
        pytz \
        pyyaml \
        scrapy \
        simplejson \
        SQLAlchemy \
        sqlparse \
        tabulate \
        gxx_linux-64 \
        make

#Installation of pylucene referenced from
#https://bitbucket.org/coady/docker/src/tip/pylucene/Dockerfile?fileviewer=file-view-default
#Edited to use pylucene 4.10.1
RUN echo 'deb http://httpredir.debian.org/debian jessie-backports main' > /etc/apt/sources.list.d/jessie-backports.list
RUN apt-get update \
    && apt-get install -y ca-certificates-java='20161107~bpo8+1' openjdk-8-jdk \
    && apt-get install -y ant build-essential nano jq

WORKDIR /usr/src/pylucene
RUN curl https://www.apache.org/dist/lucene/pylucene/pylucene-4.10.1-1-src.tar.gz \
    | tar -xz --strip-components=1
RUN cd jcc \
    && JCC_JDK=/usr/lib/jvm/java-8-openjdk-amd64 python setup.py install
RUN make all install JCC='python -m jcc' ANT=ant PYTHON=python NUM_FILES=8

WORKDIR ..
RUN rm -rf pylucene

# RUN git clone --recursive --depth=1 https://github.com/IUNetSci/hoaxy-backend.git \
#     && cd hoaxy-backend \
#     && python setup.py install
COPY . .
RUN python setup.py install

ENV HOAXY_HOME=/home/.hoaxy

EXPOSE 5432
