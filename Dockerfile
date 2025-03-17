# Building the binary of the App
FROM golang:1.23-alpine AS build



# `farmako-abdm` should be replaced with your project name
WORKDIR /go/src/layeredge-lb

# Copy changes in project to the build directory
COPY go.mod .
COPY go.sum .

# Downloads all the dependencies in advance (could be left out, but it's more clear this way)
RUN go mod download

# Copy all the Code and stuff to compile everything
COPY . .

# Builds the application as a staticly linked one, to allow it to run on alpine
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o app .


# Exposes port 8080 because our program listens on that port
EXPOSE 8080

CMD ["./app"]