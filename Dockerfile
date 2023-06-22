# multistage build
FROM golang:1.19-alpine AS build
WORKDIR /go/src/proglog
COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/proglog ./cmd/proglog
RUN go install github.com/grpc-ecosystem/grpc-health-probe@latest

FROM scratch
# copy files from the build-stage container
COPY --from=build /go/bin/proglog /bin/proglog
COPY --from=build /go/bin/grpc-health-probe /bin/grpc_health_probe
ENTRYPOINT [ "/bin/proglog" ]
