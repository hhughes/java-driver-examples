FROM ubuntu:18.04
#FROM maven:3.5.3-jdk-8-alpine
RUN apt-get update && \
    apt-get install iproute2 iputils-ping maven -y
COPY ./. /