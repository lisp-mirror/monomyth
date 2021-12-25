FROM debian:buster
MAINTAINER Paul Ricks <pauljricks@gmail.com>

ARG SBCL_VERSION=sbcl/2.1.11
ARG ROS_VERSION=21.10.14.111

RUN apt update -qy && \
    apt install -y curl libcurl4-openssl-dev libcurl3-gnutls automake build-essential librabbitmq-dev libzmq3-dev libffi-dev libev-dev pkg-config

RUN curl -L "https://github.com/roswell/roswell/releases/download/v${ROS_VERSION}/roswell_${ROS_VERSION}-1_amd64.deb" --output roswell.deb && \
    dpkg -i roswell.deb

RUN ros run -- --version

RUN ros install qlot
