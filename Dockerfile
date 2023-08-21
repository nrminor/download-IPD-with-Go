# Start from the official Go image
FROM golang:1.17

# Set the working directory inside the container
WORKDIR /app

# Copy the local package files to the container's workspace
COPY . .

# Build the Go app inside the container
RUN go build -o goDownloadIPD

# Command to run the application
CMD ["./goDownloadIPD"]
