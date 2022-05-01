FROM loumstarlearjet/aws-brand-sentiment-base-image:latest

# Create virtual env for app within /tmp
RUN python3.8 -m venv app
# Set the app working directory within the venv
WORKDIR /tmp/app/

# Install python libs
COPY requirements.txt .
RUN . ./bin/activate && \
    python3.8 -m pip install -r requirements.txt

# Copy over the application and tests
COPY extractor/ extractor/
COPY main.py .

# Copy spark log4j config so only warnings are displayed
COPY log4j.properties /opt/spark/conf/log4j.properties

# Copy test script and make executable so the entrypoint
# can be overridden to run unit tests instead
COPY test.sh .
RUN chmod +x test.sh

ENTRYPOINT . ./bin/activate && spark-submit main.py