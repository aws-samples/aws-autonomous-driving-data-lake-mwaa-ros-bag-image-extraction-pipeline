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

#RUN wget \
#    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
#    && mkdir /root/.conda \
#    && rm -f Miniconda3-latest-Linux-x86_64.sh
##    && bash Miniconda3-latest-Linux-x86_64.sh -b \

#RUN conda --version

#COPY ./environment.yaml /app/environment.yaml
#RUN conda env create -f /app/environment.yaml
# Make RUN commands use the new environment:
#SHELL ["conda", "run", "-n", "py3env", "/bin/bash", "-c"]
#RUN conda init bash
#RUN /root/miniconda3/bin/activate py3env

COPY ./app/requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

COPY ./app/env-setup.sh /app/env-setup.sh

#COPY ./app/bag_to_csv.py /app/catkin_ws/src/AVData/ford_demo/scripts/bag_to_csv.py
COPY ./app/parse-file.sh /app/parse-file.sh

## Run test on test data
#COPY ./app/Sample-Data.bag /app/Sample-Data.bag
#COPY ./app/topics-to-extract.yaml /app/topics-to-extract.yaml
#RUN /bin/bash -c "bash /app/parse-file.sh /app/Sample-Data.bag /app/topics-to-extract.yaml"
#RUN rm /app/Sample-Data.bag
#RUN rm /app/topics-to-extract.yaml

COPY ./app/engine.py /app/engine.py
COPY ./app/main.py /app/main.py

COPY ./app/export.launch /app/export.launch
copy ./app/run.sh /app/run.sh

WORKDIR /app

ENTRYPOINT ["/app/run.sh"]
