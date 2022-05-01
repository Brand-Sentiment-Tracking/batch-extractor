FROM loumstarlearjet/aws-brand-sentiment-base-image:latest

WORKDIR /tmp/
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