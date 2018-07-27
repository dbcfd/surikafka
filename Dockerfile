FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y curl
RUN apt-get install -y openssl libssl-dev
RUN apt-get install -y pkg-config
RUN apt-get install -y zlib1g-dev
RUN apt-get install -y python

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain nightly
ENV PATH /root/.cargo/bin/:$PATH

WORKDIR /surikafka
ADD /src src
ADD /Cargo.toml .