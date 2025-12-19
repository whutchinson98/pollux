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

create-localstack:
    docker run -d --name localstack -p 4566:4566 -e SERVICES=sqs localstack/localstack

create-queue:
    AWS_REGION=us-east-1 aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name sqs-worker-queue

populate-queue:
	#!/usr/bin/env bash
	for i in {1..25}; do
		body=$((RANDOM % 25 + 1))
		AWS_REGION=us-east-1 aws --endpoint-url=http://localhost:4566 sqs send-message --queue-url http://localhost:4566/000000000000/sqs-worker-queue --message-body "$body"
	done

purge-queue:
    AWS_REGION=us-east-1 aws --endpoint-url=http://localhost:4566 sqs purge-queue --queue-url http://localhost:4566/000000000000/sqs-worker-queue
