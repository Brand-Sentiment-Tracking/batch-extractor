FROM amazonlinux:latest

ENV PYTHONVER 3.6.15

RUN yum -y groupinstall "Development tools"
RUN yum -y install zlib-devel
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
RUN pip3.6 install virtualenv

WORKDIR /root
RUN rm -r /tmp/Python-${PYTHONVER}

WORKDIR /tmp/

COPY requirements.txt .

RUN pip3.6 install --upgrade pip
RUN pip3.6 install -r requirements.txt

COPY extractor.py .

ENV WARC_DIRECTORY          '/tmp/warcs/'
ENV ARTICLE_DIRECTORY       '/tmp/articles/'

ENV VALID_HOSTS             '["bbc.co.uk"]'

ENV S3_BUCKET_NAME          'bucket-name'
ENV AWS_ACCESS_KEY_ID       'access-id'
ENV AWS_SECRET_ACCESS_KEY   'secret-key'

ENV ENVIRONMENT_TYPE        'dev'

EXPOSE 8080
EXPOSE 443

ENTRYPOINT [ "python3.6", "extractor.py" ]