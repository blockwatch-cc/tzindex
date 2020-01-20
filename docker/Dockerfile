# build stage
FROM          golang:alpine AS builder
ARG           REPO=blockwatch.cc/tzindex
ARG           BUILD_TARGET=tzindex
ARG           BUILD_VERSION=dev
ARG           BUILD_COMMIT=none
LABEL         autodelete="true"
ADD           . /go/src/${BUILD_TARGET}
WORKDIR       /go/src/${BUILD_TARGET}
RUN           apk --no-cache add git binutils
RUN           go mod download
RUN           CGO_ENABLED=0 go build -a -o /${BUILD_TARGET} -ldflags "-w -X ${REPO}/cmd.VERSION=${BUILD_VERSION} -X ${REPO}/cmd.GITCOMMIT=${BUILD_COMMIT}" ${BUILD_TARGET}.go
RUN           strip /${BUILD_TARGET}

# final stage
FROM          alpine:latest
MAINTAINER    Alexander Eichhorn <alex@kidtsunami.com>
ARG           BUILD_TARGET=tzindex
ARG           USER=tzindex
ENV           BUILD_TARGET=${BUILD_TARGET}
RUN           apk --no-cache add ca-certificates 'su-exec>=0.2' \
              && addgroup $USER -g 500 \
			  && adduser -u 500 -D -h /home/$USER -S -s /sbin/nologin -G $USER $USER
COPY          --from=builder /${BUILD_TARGET} /usr/local/bin
COPY          docker/docker-entrypoint.sh /usr/local/bin/
COPY          docker/config.json /home/$USER/
RUN           chmod +x /usr/local/bin/${BUILD_TARGET}
WORKDIR       /home/$USER
EXPOSE        8000
ENTRYPOINT    ["docker-entrypoint.sh"]
CMD           ["", "run"]