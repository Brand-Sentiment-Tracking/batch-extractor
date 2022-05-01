FROM amazoncorretto:8

ARG PYTHON_VERSION=3.8.12
ARG SPARK_VERSION=3.1.3
ARG HADOOP_VERSION_SHORT=3.2
ARG HADOOP_VERSION=3.2.0
ARG AWS_SDK_VERSION=1.11.375

ARG SPARK_DIRNAME=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT}

ARG PYTHON_URL=https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
ARG SPARK_URL=https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT}.tgz
ARG HADOOP_AWS_URL=https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar
ARG AWS_SDK_URL=https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

RUN yum update kernel

RUN yum -y groupinstall "Development tools"
RUN yum -y install zlib-devel libffi-devel
RUN yum -y install bzip2-devel openssl-devel ncurses-devel
RUN yum -y install sqlite sqlite-devel xz-devel
RUN yum -y install readline-devel tk-devel gdbm-devel db4-devel
RUN yum -y install libpcap-devel xz-devel
RUN yum -y install libjpeg-devel
RUN yum -y install wget

WORKDIR /tmp/

RUN wget --no-check-certificate ${PYTHON_URL}
RUN tar -zxvf Python-${PYTHON_VERSION}.tgz

WORKDIR /tmp/Python-${PYTHON_VERSION}
RUN ./configure --prefix=/usr/local LDFLAGS="-Wl,-rpath /usr/local/lib" --with-ensurepip=install
RUN make && make altinstall

WORKDIR /tmp/
RUN rm -r /tmp/Python-${PYTHON_VERSION}

# Download and extract Spark
RUN wget -qO- ${SPARK_URL} | tar zx -C /opt && \
    mv /opt/${SPARK_DIRNAME} /opt/spark

# Add hadoop-aws and aws-sdk
RUN wget ${HADOOP_AWS_URL} -P /opt/spark/jars/ && \
    wget ${AWS_SDK_URL} -P /opt/spark/jars/

ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3.8
ENV PATH=${SPARK_HOME}/bin:${PATH}

RUN python3.8 -m pip install --upgrade pip setuptools
RUN python3.8 -m pip install virtualenv

# Create virtual env for app
RUN python3.8 -m venv app

# Set the app working directory within the venv
WORKDIR /tmp/app/

# Install python libs
COPY requirements.txt .

RUN . ./bin/activate && \
    python3.8 -m pip install -r requirements.txt

COPY extractor/ extractor/
COPY main.py .

# Copy spark log4j config so only warnings are displayed
COPY log4j.properties /opt/spark/conf/log4j.properties
# Copy shell script for running tests (overwrite entrypoint)
COPY test.sh .

RUN chmod +x test.sh

ENTRYPOINT . ./bin/activate && spark-submit main.py