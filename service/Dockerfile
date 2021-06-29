FROM ros:melodic-perception-bionic

RUN rm /bin/sh; ln -s /bin/bash /bin/sh
RUN apt update && apt-get install python3-pip unzip curl vim ffmpeg -y 
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN sudo ./aws/install


RUN apt install git -y
RUN apt-get install ros-melodic-pcl-conversions -y
RUN apt-get install ros-melodic-pcl-ros -y

RUN mkdir /app

# Install Python3
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"

RUN apt-get update
RUN apt-get install -y wget && rm -rf /var/lib/apt/lists/*

COPY ./app/requirements.txt /app/requirements.txt
RUN pip3 install --upgrade cython
RUN pip3 install -r /app/requirements.txt

COPY ./app/env-setup.sh /app/env-setup.sh

COPY ./app/parse-file.sh /app/parse-file.sh

COPY ./app/engine.py /app/engine.py
COPY ./app/main.py /app/main.py

COPY ./app/export.launch /app/export.launch
COPY ./app/run.sh /app/run.sh

WORKDIR /app

ENTRYPOINT ["/app/run.sh"]
