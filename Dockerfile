FROM ubuntu:bionic
MAINTAINER Paul Ricks <pauljricks@gmail.com>

ENV PATH "$PATH:/root/.roswell/bin"

RUN apt update -qy && \
    apt install -y curl libcurl4-openssl-dev automake build-essential librabbitmq-dev libzmq3-dev libffi-dev libev-dev && \
    curl -L https://raw.githubusercontent.com/roswell/roswell/release/scripts/install-for-ci.sh | sh && \
    ros run -- --version && \
    ros install qlot
