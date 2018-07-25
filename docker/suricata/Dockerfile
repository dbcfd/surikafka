#Docker Build for Suricata C Executable
FROM ubuntu:16.04 AS builder

ENV VERSION=4.1.0

LABEL version=$VERSION
LABEL description="Suricata with debian package"

ENV BUILD_DIR=/build \
    OUTPUT_DIR=/output \
    ARTIFACTS_DIR=/artifacts

RUN mkdir -p $BUILD_DIR \
    && mkdir -p $OUTPUT_DIR

#Setup required install directories
WORKDIR /
RUN mkdir -p usr/local/include \
  && mkdir -p usr/lib \
  && mkdir -p usr/bin \
  && mkdir -p var/run/suricata \
  && mkdir -p etc

#Setup required package directories
WORKDIR $OUTPUT_DIR
RUN mkdir -p usr/local/include \
  && mkdir -p usr/lib \
  && mkdir -p usr/bin \
  && mkdir -p var/run/suricata \
  && mkdir -p etc

#Install required libraries
RUN apt-get update && apt-get install -y \
  g++ \
  dh-make \
  git-core \
  debhelper \
  autotools-dev \
  libpcre3-dbg \
  libpcre3-dev \
  libpcap-dev \
  libnet1-dev \
  libyaml-0-2 \
  libyaml-dev \
  zlib1g \
  zlib1g-dev \
  libcap-ng-dev \
  libcap-ng0 \
  libnss3-dev \
  libnspr4-dev \
  python \
  python-distutils-extra \
  python-setuptools \
  python-instant \
  python-support \
  libgeoip1 \
  libgeoip-dev \
  libnetfilter-queue-dev \
  libnetfilter-queue1 \
  libjansson4 \
  libjansson-dev \
  python-simplejson \
  libnfnetlink-dev \
  libnfnetlink0 \
  libmagic-dev \
  libmagic1 \
  dh-autoreconf \
  libprelude-dev \
  liblua5.1-0-dev \
  hardening-wrapper \
  wget \
  automake \
  ruby \
  ruby-dev \
  build-essential

RUN gem install fpm --no-rdoc --no-ri -v 1.3.3

#Download appropriate pcre
WORKDIR $BUILD_DIR
RUN wget http://sourceforge.net/projects/pcre/files/pcre/8.36/pcre-8.36.tar.bz2/download -O pcre.tar.bz2 \
  && bunzip2 pcre.tar.bz2 \
  && tar -xvf pcre.tar

#Install pcre
WORKDIR $BUILD_DIR
RUN cd pcre-8.36 \
  && ls -l configure \
  && sh configure --enable-jit --libdir=/usr/lib/ \
  && make \
  && make install \
  && make install DESTDIR=$OUTPUT_DIR \
  && rm -rf pcre-8.36

#Checkout suricata
RUN git clone https://github.com/oisf/suricata suricata

WORKDIR suricata

# libhtp stuffs here
RUN git clone https://github.com/ironbee/libhtp

RUN sh autogen.sh \
  && sh configure --prefix=/usr/ --sysconfdir=/etc/ --localstatedir=/var/ --enable-unix-socket --enable-pcre-jit \
  --with-libpcre-includes=/usr/local/include --with-libpcre-libraries=/usr/lib \
  && make \
  && make install DESTDIR=$OUTPUT_DIR \
  && make install-conf DESTIDR=$OUTPUT_DIR \
  && rm -rf $BUILD_DIR

RUN mkdir -p $ARTIFACTS_DIR

WORKDIR $ARTIFACTS_DIR

ENV FPM_NAME="suricata-"$VERSION"_amd64.deb"

RUN /usr/local/bin/fpm -s dir -t deb -n suricata -v $VERSION -C $OUTPUT_DIR \
  -d libnet1 -d libjansson4 -d libyaml-0-2 -d libnss3 \
  -p $FPM_NAME usr/bin usr/lib etc var

FROM ubuntu:16.04 AS runner

RUN find $ARTIFACTS_DIR -name "suricata*_amd64.deb" -type f -exec dpkg -i {} \;

VOLUME /pcaps

ENTRYPOINT ["/usr/bin/suricata"]

CMD ["-R", "/pcaps"]
