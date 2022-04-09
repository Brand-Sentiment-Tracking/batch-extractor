FROM amazonlinux:latest

ENV PYTHONVER 3.8.12

RUN yum -y groupinstall "Development tools"
RUN yum -y install zlib-devel libffi-devel
RUN yum -y install bzip2-devel openssl-devel ncurses-devel
RUN yum -y install sqlite sqlite-devel xz-devel
RUN yum -y install readline-devel tk-devel gdbm-devel db4-devel
RUN yum -y install libpcap-devel xz-devel
RUN yum -y install libjpeg-devel
RUN yum -y install wget

WORKDIR /tmp

RUN wget --no-check-certificate https://www.python.org/ftp/python/${PYTHONVER}/Python-${PYTHONVER}.tgz
RUN tar -zxvf Python-${PYTHONVER}.tgz

WORKDIR /tmp/Python-${PYTHONVER}
RUN ./configure --prefix=/usr/local LDFLAGS="-Wl,-rpath /usr/local/lib" --with-ensurepip=install
RUN make && make altinstall

WORKDIR /root
RUN rm -r /tmp/Python-${PYTHONVER}

WORKDIR /tmp/

COPY requirements.txt .

RUN python3.8 -m pip install --upgrade pip setuptools
RUN python3.8 -m pip install virtualenv
RUN python3.8 -m pip install -r requirements.txt

COPY loader.py .
COPY extractor.py .

ENV WARC_DIRECTORY          '/tmp/warcs/'
ENV ARTICLE_DIRECTORY       '/tmp/articles/'

ENV VALID_HOSTS             '["*"]'

ENV S3_BUCKET_NAME          'bucket-name'
ENV AWS_ACCESS_KEY_ID       'access-id'
ENV AWS_SECRET_ACCESS_KEY   'secret-key'

ENV ENVIRONMENT_TYPE        'dev'

EXPOSE 8080
EXPOSE 443

ENTRYPOINT [ "python3.8", "extractor.py" ]