export RUSTFLAGS := "-Dwarnings"
export RUSTDOCFLAGS := "-Dwarnings"
export CARGO_TERM_COLOR := "always"

clippy:
    cargo clippy

clippy-fix:
    cargo clippy --fix

fmt-check:
    cargo fmt --check

fmt:
    cargo fmt

check:
    cargo check

test:
    cargo test

# SQS
create-sqs-localstack:
    docker run -d --name localstack -p 4566:4566 -e SERVICES=sqs localstack/localstack

create-sqs-queue:
    AWS_REGION=us-east-1 aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name sqs-worker-queue

populate-sqs-queue:
    #!/usr/bin/env bash
    for i in {1..25}; do
    	body=$((RANDOM % 25 + 1))
    	AWS_REGION=us-east-1 aws --endpoint-url=http://localhost:4566 sqs send-message --queue-url http://localhost:4566/000000000000/sqs-worker-queue --message-body "$body"
    done

purge-sqs-queue:
    AWS_REGION=us-east-1 aws --endpoint-url=http://localhost:4566 sqs purge-queue --queue-url http://localhost:4566/000000000000/sqs-worker-queue

run-sqs:
    SQS_QUEUE_URL=http://localhost:4566/000000000000/sqs-worker-queue cargo run --example sqs_workers

# Rabbit MQ
create-rabbitmq:
    docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
    @echo "Waiting for RabbitMQ to start..."
    @sleep 10
    curl -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/rabbitmq-worker-queue \
        -H "Content-Type: application/json" \
        -d '{"durable":true}'

populate-rabbitmq-queue:
    #!/usr/bin/env bash
    for i in {1..25}; do
    	body=$((RANDOM % 25 + 1))
    	curl -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F/amq.default/publish \
    		-H "Content-Type: application/json" \
    		-d "{\"properties\":{},\"routing_key\":\"rabbitmq-worker-queue\",\"payload\":\"$body\",\"payload_encoding\":\"string\"}"
    done

purge-rabbitmq-queue:
    curl -u guest:guest -X DELETE http://localhost:15672/api/queues/%2F/rabbitmq-worker-queue/contents

run-rabbitmq:
    RABBITMQ_URL=amqp://guest:guest@localhost:5672 RABBITMQ_QUEUE_NAME=rabbitmq-worker-queue cargo run --example rabbitmq_workers
