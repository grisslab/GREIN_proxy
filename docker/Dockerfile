FROM docker.io/python:3.10-alpine3.19

# set the location of the database
ENV GREIN_DB "/data/grein.db"

RUN mkdir "/data"

# copy the test database
COPY grein.db /data/
COPY grein_proxy*.whl /tmp/

# install the package
RUN pip install /tmp/grein_proxy*.whl && \
    rm /tmp/grein_proxy*.whl

CMD ["grein_proxy", "-p", "80", "-d", "/data/grein.db"]
