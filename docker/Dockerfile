FROM frolvlad/alpine-glibc:alpine-3.9

ENV CONDA_DIR="/opt/conda"
ENV PATH="$CONDA_DIR/bin:$PATH"

RUN apk add --no-cache --update alpine-sdk \
                     libxml2-dev \
                     libxslt-dev \
                     python-dev \
                     openssl-dev \
                     libffi-dev \
                     zlib-dev \
                     py-pip \
                     nano bash jq musl-dev

# Install conda2
# Install conda
RUN CONDA_VERSION="4.5.12" && \
    CONDA_MD5_CHECKSUM="866ae9dff53ad0874e1d1a60b1ad1ef8" && \
    \
    apk add --no-cache --virtual=.build-dependencies wget ca-certificates bash && \
    \
    mkdir -p "$CONDA_DIR" && \
    wget "http://repo.continuum.io/miniconda/Miniconda3-${CONDA_VERSION}-Linux-x86_64.sh" -O miniconda.sh && \
    echo "$CONDA_MD5_CHECKSUM  miniconda.sh" | md5sum -c && \
    bash miniconda.sh -f -b -p "$CONDA_DIR" && \
    echo "export PATH=$CONDA_DIR/bin:\$PATH" > /etc/profile.d/conda.sh && \
    rm miniconda.sh && \
    \
    conda update --all --yes && \
    conda config --set auto_update_conda False && \
    rm -r "$CONDA_DIR/pkgs/" && \
    \
    apk del --purge .build-dependencies && \
    \
    mkdir -p "$CONDA_DIR/locks" && \
    chmod 777 "$CONDA_DIR/locks"

ENV JAVA_HOME=/usr/lib/jvm/default-jvm
RUN apk add --no-cache openjdk8 && \
    ln -sf "${JAVA_HOME}/bin/"* "/usr/bin/"

ENV ANT_HOME /usr/share/java/apache-ant
ENV PATH $PATH:$ANT_HOME/bin

#Install dependencies for hoaxy
RUN conda install -y python=3.7.5\
        docopt \
        Flask \
        gunicorn \
        networkx \
        pandas \
        psycopg2 \
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

WORKDIR /usr/src/pylucene

RUN apk add --no-cache --no-progress nano htop jq ca-certificates curl libssl1.1 apache-ant openssl openssl-dev g++ gcc bash git

RUN apk add --no-cache python && \
    python -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip install --upgrade pip setuptools && \
    rm -r /root/.cache

# Install Gosu to /usr/local/bin/gosu
# ADD https://github.com/tianon/gosu/releases/download/1.10/gosu-amd64 /usr/local/sbin/gosu
# RUN chmod +x /usr/local/sbin/gosu
# RUN adduser -D hoaxy -h /hoaxy -s /bin/sh
# USER hoaxy

# Install tini to /usr/local/sbin
# ADD https://github.com/krallin/tini/releases/download/v0.18.0/tini-muslc-amd64 /usr/local/sbin/tini
# RUN apk --no-cache --no-progress add ca-certificates git libssh2 openssl \
# && chmod +x /usr/local/sbin/tini && mkdir -p /opt \
# && adduser -D hoaxy -h /opt/hoaxy -s /bin/sh \
# && su hoaxy -c 'cd /opt/hoaxy; mkdir -p bin config data'

# Switch to user context
# USER hoaxy
# WORKDIR /opt/hoaxy

RUN curl https://www.apache.org/dist/lucene/pylucene/pylucene-7.7.1-src.tar.gz | tar -xz --strip-components=1 \
    && cd jcc \
    && export JCC_ARGSEP=";" \
    && export JCC_CFLAGS="-v;-fno-strict-aliasing;-Wno-write-strings;-D__STDC_FORMAT_MACROS" \
    && JCC_JDK=/usr/lib/jvm/default-jvm python setup.py install \
    && cd .. \
        && make all install JCC='python -m jcc' ANT=ant PYTHON=python NUM_FILES=8 \
        && rm -rf /usr/src/pylucene

ENV HOAXY_VCS_BRANCH=${HOAXY_VCS_BRANCH:-"master"} \
    HOAXY_VCS_DEPTH=${HOAXY_VCS_DEPTH:-"1"} \
    HOAXY_VCS_REMOTE=${HOAXY_VCS_REMOTE:-"https://github.com/IUNetSci/hoaxy-backend.git"}

RUN apk add --no-cache --no-progress yaml-dev postgresql-dev \
    && git clone --recursive --depth=${HOAXY_VCS_DEPTH} --branch ${HOAXY_VCS_BRANCH} ${HOAXY_VCS_REMOTE} \
    && cd hoaxy-backend 
#    && conda install -y psycopg2

RUN cd hoaxy-backend \
    && pip install --upgrade --force-reinstall jieba3k==0.35.1 \
    && python setup.py install

#    && pip install --upgrade --force-reinstall setuptools \
#    && find . -name '*.pyc' -delete \

## Use entrypoint script to switch case and pass custom/default params to hoaxy
WORKDIR /opt
COPY entrypoint.sh .
RUN chmod +x /opt/entrypoint.sh

## If Dockerfile at root dir of vcs repo
# COPY . . 
# RUN python setup.py install

ENV HOAXY_HOME=/home/.hoaxy

EXPOSE 5432

## If entrypoint.sh switch case
ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["bash"]
## Dev mode
# CMD ["/bin/bash"]

## With tini example
# ENTRYPOINT ["tini", "-g", "--"]
# CMD ["hoaxy", "-conf_dir=/opt/hoaxy/config"]
