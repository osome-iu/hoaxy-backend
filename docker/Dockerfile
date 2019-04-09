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
                     nano bash jq

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

# java openjdk
ENV JAVA_VERSION=8 \
    JAVA_UPDATE=202 \
    JAVA_BUILD=08 \
    JAVA_PATH=1961070e4c9b4e26a04e7f5a083f551e \
    JAVA_HOME="/usr/lib/jvm/default-jvm"

RUN apk add --no-cache --virtual=build-dependencies wget ca-certificates unzip && \
    cd "/tmp" && \
    wget --header "Cookie: oraclelicense=accept-securebackup-cookie;" \
        "http://download.oracle.com/otn-pub/java/jdk/${JAVA_VERSION}u${JAVA_UPDATE}-b${JAVA_BUILD}/${JAVA_PATH}/jdk-${JAVA_VERSION}u${JAVA_UPDATE}-linux-x64.tar.gz" && \
    tar -xzf "jdk-${JAVA_VERSION}u${JAVA_UPDATE}-linux-x64.tar.gz" && \
    mkdir -p "/usr/lib/jvm" && \
    mv "/tmp/jdk1.${JAVA_VERSION}.0_${JAVA_UPDATE}" "/usr/lib/jvm/java-${JAVA_VERSION}-oracle" && \
    ln -s "java-${JAVA_VERSION}-oracle" "$JAVA_HOME" && \
    ln -s "$JAVA_HOME/bin/"* "/usr/bin/" && \
    rm -rf "$JAVA_HOME/"*src.zip && \
    rm -rf "$JAVA_HOME/lib/missioncontrol" \
           "$JAVA_HOME/lib/visualvm" \
           "$JAVA_HOME/lib/"*javafx* \
           "$JAVA_HOME/jre/lib/plugin.jar" \
           "$JAVA_HOME/jre/lib/ext/jfxrt.jar" \
           "$JAVA_HOME/jre/bin/javaws" \
           "$JAVA_HOME/jre/lib/javaws.jar" \
           "$JAVA_HOME/jre/lib/desktop" \
           "$JAVA_HOME/jre/plugin" \
           "$JAVA_HOME/jre/lib/"deploy* \
           "$JAVA_HOME/jre/lib/"*javafx* \
           "$JAVA_HOME/jre/lib/"*jfx* \
           "$JAVA_HOME/jre/lib/amd64/libdecora_sse.so" \
           "$JAVA_HOME/jre/lib/amd64/"libprism_*.so \
           "$JAVA_HOME/jre/lib/amd64/libfxplugins.so" \
           "$JAVA_HOME/jre/lib/amd64/libglass.so" \
           "$JAVA_HOME/jre/lib/amd64/libgstreamer-lite.so" \
           "$JAVA_HOME/jre/lib/amd64/"libjavafx*.so \
           "$JAVA_HOME/jre/lib/amd64/"libjfx*.so && \
    rm -rf "$JAVA_HOME/jre/bin/jjs" \
           "$JAVA_HOME/jre/bin/keytool" \
           "$JAVA_HOME/jre/bin/orbd" \
           "$JAVA_HOME/jre/bin/pack200" \
           "$JAVA_HOME/jre/bin/policytool" \
           "$JAVA_HOME/jre/bin/rmid" \
           "$JAVA_HOME/jre/bin/rmiregistry" \
           "$JAVA_HOME/jre/bin/servertool" \
           "$JAVA_HOME/jre/bin/tnameserv" \
           "$JAVA_HOME/jre/bin/unpack200" \
           "$JAVA_HOME/jre/lib/ext/nashorn.jar" \
           "$JAVA_HOME/jre/lib/jfr.jar" \
           "$JAVA_HOME/jre/lib/jfr" \
           "$JAVA_HOME/jre/lib/oblique-fonts" && \
    wget --header "Cookie: oraclelicense=accept-securebackup-cookie;" \
        "http://download.oracle.com/otn-pub/java/jce/${JAVA_VERSION}/jce_policy-${JAVA_VERSION}.zip" && \
    unzip -jo -d "${JAVA_HOME}/jre/lib/security" "jce_policy-${JAVA_VERSION}.zip" && \
    rm "${JAVA_HOME}/jre/lib/security/README.txt" && \
    apk del build-dependencies && \
    rm "/tmp/"* && \
    \
    echo 'public class Main { public static void main(String[] args) { System.out.println("Java code is running fine!"); } }' > Main.java && \
    javac Main.java && \
    java Main && \
    rm -r "/tmp/"*

ENV ANT_HOME /usr/share/java/apache-ant
ENV PATH $PATH:$ANT_HOME/bin

#Install dependencies for hoaxy
RUN conda install -y \
        docopt \
        Flask \
        gunicorn \
        networkx \
        pandas \
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

RUN curl https://www.apache.org/dist/lucene/pylucene/pylucene-7.6.0-src.tar.gz | tar -xz --strip-components=1 \
    && cd jcc \
    && JCC_JDK=/usr/lib/jvm/default-jvm python setup.py install \
    && cd .. \
        && make all install JCC='python -m jcc' ANT=ant PYTHON=python NUM_FILES=8 \
        && rm -rf /usr/src/pylucene

ENV HOAXY_VCS_BRANCH=${HOAXY_VCS_BRANCH:-"hoaxy-python3"} \
    HOAXY_VCS_DEPTH=${HOAXY_VCS_DEPTH:-"1"} \
    HOAXY_VCS_REMOTE=${HOAXY_VCS_REMOTE:-"https://github.com/IUNetSci/hoaxy-backend.git"}

RUN git clone --recursive --depth=${HOAXY_VCS_DEPTH} --branch ${HOAXY_VCS_BRANCH} ${HOAXY_VCS_REMOTE} \
    && cd hoaxy-backend \
    && conda install -y psycopg2 \
    && pip install -U psycopg2 \
    && python setup.py install

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
