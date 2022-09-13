FROM python:3.10.7-alpine3.16 as base

# build and load all requirements
FROM base as builder
WORKDIR /app

# upgrade pip to the latest version
RUN apk --no-cache upgrade \
    && pip install --no-cache-dir --upgrade pip \
    && apk --no-cache add tzdata build-base


COPY setup.py ./
# install necessary packages to a temporary folder
RUN apk add --no-cache libffi-dev "librdkafka-dev" && pip install --no-cache-dir --prefix=/install .

# build a clean environment
FROM base
WORKDIR /app

# copy all loaded and built libraries to a pure basic image
COPY --from=builder /install /usr/local
# add default timezone settings
COPY --from=builder /usr/share/zoneinfo/Etc/UTC /etc/localtime
RUN echo "Etc/UTC" > /etc/timezone

# bash is installed for more convenient debugging.
RUN apk --no-cache add bash \
    && apk --no-cache add git \
    && apk --no-cache add libffi-dev "librdkafka-dev"

# copy payload code only
COPY * ./

ENTRYPOINT ["python", "/app/producer.py"]
