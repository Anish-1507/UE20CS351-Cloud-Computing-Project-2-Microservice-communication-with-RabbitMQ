# basic python image
FROM python:3.7

COPY requirements.txt .

# install pika to access rabbitmq
RUN pip install -r requirements.txt

# Without this setting, Python never prints anything out.
ENV PYTHONUNBUFFERED=1

# declare the source directory
WORKDIR /usr/src/app/consumer_two

# copy the file
COPY insertion.py .

# start command
CMD [ "python", "insertion.py" ]
