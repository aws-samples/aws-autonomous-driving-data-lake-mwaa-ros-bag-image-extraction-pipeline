FROM ros:melodic-perception-bionic

RUN sudo apt-key adv --keyserver 'hkp://keyserver.ubuntu.com:80' --recv-key C1CF6E31E6BADE8868B172B4F42ED6FBAB17C654
RUN sudo apt-key del 421C365BD9FF1F717815A3895523BAEEB01FA116
RUN rm /bin/sh; ln -s /bin/bash /bin/sh
RUN apt update 
RUN apt-get install git \
                    python3-pip \
                    unzip \
                    curl \
                    wget \
                    vim \
                    ffmpeg \
                    ros-melodic-pcl-conversions \
                    ros-melodic-pcl-ros \
                    libsnappy-dev -y
RUN wget http://launchpadlibrarian.net/644388030/linux-libc-dev_5.15.0-58.64_amd64.deb
RUN dpkg -i linux-libc-dev_5.15.0-58.64_amd64.deb
RUN rm -rf /var/lib/apt/lists/*

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN sudo ./aws/install

RUN mkdir /app

# Install Python3
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"

COPY ./app/requirements.txt /app/requirements.txt
RUN pip3 install --upgrade cython lxml
RUN pip3 install -r /app/requirements.txt

COPY ./app/env-setup.sh /app/env-setup.sh

COPY ./app/parse-file.sh /app/parse-file.sh

COPY ./app/engine.py /app/engine.py
COPY ./app/main.py /app/main.py

COPY ./app/export.launch /app/export.launch
COPY ./app/run.sh /app/run.sh

WORKDIR /app

ENTRYPOINT ["/app/run.sh"]
