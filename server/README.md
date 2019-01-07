## Development Setup

1. Using Go 1.11
2. mkdir -p /tmp/badger/logs && mkdir -p /tmp/badger/offsets
3. go build . (and `go get` whatever is missing)
4. ./server

## API

Retrieve all topics:
`curl -v '127.0.0.1:9000/topics`

Post message "bar" to topic "foo":
`curl -vX POST 127.0.0.1:9000/topics/foo -H 'Content-Type: application/json' -d '{"Message": "bar"}'`

Retrieve a message from topic "foo":
`curl -vX GET '127.0.0.1:9000/a?group=a&client_id=1'`
  - `group`: the message group this client belongs to (messages are delivered round robin in a group)
  - `client_id`: a unique ID for the client
