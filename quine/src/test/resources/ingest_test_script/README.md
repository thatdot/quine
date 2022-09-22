# Ingest test utility

A utility script for testing external stream-based systems.

This scirpt is intended to simulate and test stream ingestion for Kafka, Kinesis and SQS streams. 

## Requirements

	- A running instance of quine
	- A running instance of the required external resource

## Invocation
`python ingest_test.py [type] -h` where `type` is one of  `kafka`, `kinesis`, `sqs`

### kinesis:
	Kinesis requires the stream name to be provided as well as AWS configuration:
```bash
export AWS_REGION=...
export AWS_KEY=...
export AWS_SECRET=xbGoorGbsvfryIqTn5szqWlv4qgiuxPNeNBzM42b...
python ingest_test.py kinesis --name ingest-test-stream --region $AWS_REGION --key $AWS_KEY --secret $AWS_SECRET
```

### sqs
	SQS requires the queue name to be provided as well as AWS configuration:
	
	`python ingest_test.py  sqs -q test_ingest_queue --region $AWS_REGION --key $AWS_KEY --secret $AWS_SECRET`

### kafka
	Kafka requires a valid kafka instance URL as well as a valid topic:
	
	`python ingest_test.py  kafka -t test_topic -u localhost:9092`
	
## Operation
	
	This script works by generating a random key for each run and generating N json data elements containg that key. We then run
	an  ingest and a query for values containing that generated key. Each run therefore generates N new values in quine. The 
	number of generated values is configurable but defaults to 10.

## Limitations
	
	- Currently only testing Json ingest. 
	- Only testing that ingest properly reads values into the Quine graph. Not testing things like stream offsets, throttling, optional parameters, ...
 	


