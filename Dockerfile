FROM amazoncorretto:8

ARG SPARK_VERSION=3.1.3
ARG HADOOP_VERSION_SHORT=3.2
ARG HADOOP_VERSION=3.2.0
ARG AWS_SDK_VERSION=1.11.375
ENV PYTHON_VERSION=3.8.12

RUN yum -y groupinstall "Development tools"
RUN yum -y install zlib-devel libffi-devel
RUN yum -y install bzip2-devel openssl-devel ncurses-devel
RUN yum -y install sqlite sqlite-devel xz-devel
RUN yum -y install readline-devel tk-devel gdbm-devel db4-devel
RUN yum -y install libpcap-devel xz-devel
RUN yum -y install libjpeg-devel
RUN yum -y install wget

WORKDIR /tmp/

RUN wget --no-check-certificate https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
RUN tar -zxvf Python-${PYTHON_VERSION}.tgz

WORKDIR /tmp/Python-${PYTHON_VERSION}
RUN ./configure --prefix=/usr/local LDFLAGS="-Wl,-rpath /usr/local/lib" --with-ensurepip=install
RUN make && make altinstall

WORKDIR /tmp/
RUN rm -r /tmp/Python-${PYTHON_VERSION}

# Download and extract Spark
RUN wget -qO- https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT}.tgz | tar zx -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT} /opt/spark

# Configure Spark to respect IAM role given to container
RUN echo spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper > /opt/spark/conf/spark-defaults.conf

# Add hadoop-aws and aws-sdk
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P /opt/spark/jars/ && \ 
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P /opt/spark/jars/

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

COPY loader.py .
COPY extractor.py .

ENV URL_PATTERNS            '["*"]'

ENV S3_BUCKET_NAME          'bucket-name'
ENV AWS_ACCESS_KEY_ID       'access-id'
ENV AWS_SECRET_ACCESS_KEY   'secret-key'

ENV ENVIRONMENT_TYPE        'dev'

EXPOSE 8080
EXPOSE 443

ENTRYPOINT . ./bin/activate && \
           echo 127.0.0.1 $HOSTNAME >> /etc/hosts && \
           spark-submit extractor.py