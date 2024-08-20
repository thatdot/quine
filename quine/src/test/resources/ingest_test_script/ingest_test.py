import argparse
import json
from pykafka import KafkaClient
import string
import random
from typing import *
import requests
from requests import Response
from termcolor import colored
import logging
import boto3
import time

import gzip
import zlib
import base64

logging.basicConfig(level=logging.INFO)

ENCODINGS = ["Gzip", "Zlib", "Base64"]

class Encoding:

    @classmethod
    def parse_csv(cls, encoding_csv: str):
        encoding_strings = encoding_csv and [s.strip() for s in encoding_csv.split(',')] or []
        diff = (set(encoding_strings)).difference(set(ENCODINGS))
        if len(diff) > 0:
            raise Exception(f"The encodings {diff} were not recognized. Only the strings {ENCODINGS} are supported.")
        return list(filter(lambda e: e in ENCODINGS, encoding_strings))

    @classmethod
    def encode_value(cls, encoding: str, value: Any):
        if encoding == "Gzip":
            return gzip.compress(value)
        elif encoding == "Zlib":
            return zlib.compress(value)
        elif encoding == "Base64":
            return base64.b64encode(value)

    @classmethod
    def decode_value(cls, encoding: str, value: Any):
        if encoding == "Gzip":
            return gzip.decompress(value)
        elif encoding == "Zlib":
            return zlib.decompress(value)
        elif encoding == "Base64":
            return base64.b64decode(value)

    @classmethod
    def encode(cls, encodings: List[str], value: str) ->str:
        bytes = value.encode("utf-8")
        for e in encodings[::-1]:
            bytes = cls.encode_value(e, bytes)
        return bytes.decode("utf-8")

    @classmethod
    def decode(cls, encodings: List[str], value: str) ->str:
        for e in encodings:
            value = cls.decode_value(e, value)
        return value


def random_string(ct: int = 10):
    return ''.join(random.choice(string.ascii_letters) for i in range(ct))


class TestConfig:

    def __init__(self, name:str, count: int, quine_url: str, encodings: List[str]):
        self.name = name
        self.quine_url = quine_url
        self.count = count
        self.encodings = encodings
    def recipe(self):
        pass

    def generate_values(self):
        raw_values = [{"test_name": self.name, "counter": i, "key": f"{i}_{self.name}"} for i in range(self.count)]
        return list(map(lambda rec: Encoding.encode(self.encodings, json.dumps(rec)), raw_values))

    def write_values(self, values: List[Any]) -> None:
        pass

    def create_recipe(self):
        print(colored("Sending recipe", "magenta"))
        self.req("post", f'/api/v1/ingest/{self.name}',
                 json=self.recipe() | {
                     "recordDecoders": self.encodings})  # , headers={"Content-type":"application/json"})

    def retrieve_values(self):
        return self.req("post", f'/api/v1/query/cypher/nodes',
                        data=f"MATCH (n) WHERE n.test_name = '{self.name}' RETURN n LIMIT {self.count}",
                        headers={"Content-type": "text/plain"}).json()

    def get_ingested_ct(self):
        rsp = self.req("get", f'/api/v1/ingest/{self.name}').json()
        return rsp["stats"]["ingestedCount"]

    def run_test(self, sleep_time_ms=20000, write=True, read = True):
        print(f"READ={read} WRITE={write}")
        if read:
            self.create_recipe()
        print(f"sleeping {sleep_time_ms/1000.0} seconds before writing values")
        time.sleep(sleep_time_ms / 1000.0)
        if write:
            values = self.generate_values()
            self.write_values(values)
        time.sleep(sleep_time_ms / 1000.0)
        if read:
            returned_values = self.retrieve_values()
            if (len(returned_values) == self.count):
                print(colored(f"Correct number of values ({self.count}) received from type {self.recipe()['type']}", "green"))
            else:
                print(colored(f"Expected {self.count} values, got {len(returned_values)}", "red"))

            for r in returned_values:
                assert(r["properties"]["test_name"] == self.name)

            print(colored(f"returned values are in the correct form: {returned_values[0]}", "green"))
            assert len(returned_values) == self.count

    def req(self, method: str, path: str, **kwargs) -> Optional[Response]:
        url = f'http://{self.quine_url}{path}'
        print(colored(f"call {method} {url} {kwargs}", "blue"))
        response = requests.request(method, f'http://{self.quine_url}{path}', **kwargs)

        if response.ok:
            print(colored(f"Success: {method} {url} {response.status_code}", "green"))
            # logging.debug("%s %s %s", method, url, response.status_code)
            try:
                logging.debug(json.dumps(response.json(), indent=2))
            except:
                pass
        else:
            print(colored(f"Fail: {method} {url} {response.status_code} \n{response._content}", "red"))

        return response


class KinesisConfig(TestConfig):

    def __init__(self, name: str, count: int, quine_url: str, stream_name: str, encodings: List[str], checkpoint_batch_size:Optional[int], checkpoint_batch_wait_ms:Optional[int], creds: Dict[str, str]):
        super().__init__(name, count, quine_url, encodings)
        self.stream_name = stream_name
        self.creds = creds
        self.checkpoint_settings =  { "checkpointSettings":{  "maxBatchSize":checkpoint_batch_size,  "maxBatchWait":checkpoint_batch_wait_ms }}  if checkpoint_batch_size else None

    def recipe(self):

        base_value= {"name": self.name,
                "type": "KinesisIngest",
                "format": {"query": "CREATE ($that)", "type": "CypherJson"},
                "streamName": self.stream_name,
                "credentials": {"region": self.creds["region"],
                                "accessKeyId": self.creds["key"],
                                "secretAccessKey": self.creds["secret"]}}
        if self.checkpoint_settings:
            base_value.update(self.checkpoint_settings)

        return base_value

    def write_values(self, values: List[str]):
        kinesis_client = boto3.client('kinesis')
        kinesis_client.put_records(StreamName=self.stream_name,
                                   Records=[{"Data": v, "PartitionKey": "test_name"} for v in values])

class SQSConfig(TestConfig):
    def __init__(self, name: str,count: int, quine_url: str, queue_url: str, encodings: List[str], creds: Dict[str, str]):
        super().__init__(name, count, quine_url, encodings)
        self.queue_url = queue_url
        self.creds = creds

    def recipe(self):
        return {"name": self.name,
                "type": "SQSIngest",
                "format": {"query": "CREATE ($that)", "type": "CypherJson"},
                "queueUrl": self.queue_url,
                "credentials": {"region": self.creds["region"],
                                "accessKeyId": self.creds["key"],
                                "secretAccessKey": self.creds["secret"]}}

    def write_values(self, values: List[str]) -> None:
        sqs_client = boto3.client("sqs", region_name=self.creds["region"])

        for value in values:
            response = sqs_client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=value
            )
            print(f"sent {value} -> {response}")
            logging.debug(response)


class KafkaConfig(TestConfig):

    def __init__(self, name: str,count: int, quine_url: str, topic: str, kafka_url: str, commit, ending_offset, encodings: List[str], waitForCommitConfirmation=True):
        super().__init__(name, count, quine_url, encodings)
        self.topic = topic
        self.kafka_url = kafka_url
        self.commit = commit
        self.ending_offset = ending_offset
        self.waitForCommitConfirmation = waitForCommitConfirmation

    def recipe(self):
        offset = (self.ending_offset and {"endingOffset": self.ending_offset}) or {}
        commit = (self.commit == "ExplicitCommit" and { "offsetCommitting": {"type":self.commit, "waitForCommitConfirmation":self.waitForCommitConfirmation, "maxIntervalMillis": 100000}}) or {}
        return {"name": self.name,
                "type": "KafkaIngest",
                "format": {"query": "CREATE ($that)", "type": "CypherJson"},
                "topics": [self.topic],
                "bootstrapServers": self.kafka_url} | offset | commit

    def write_values(self, values: List[str]):
        print(colored(f"WRITING {len(values)} VALUES", "magenta"))
        client = KafkaClient(hosts=self.kafka_url)
        topic = client.topics[self.topic]

        with topic.get_sync_producer() as producer:
            for value in self.generate_values():
                print(f"writing to {self.topic} [{value}]")
                producer.produce(value.encode("utf-8"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="ingest_tester", description="Ingest tests by type"
    )
    parser.add_argument("-q", "--quine_url", default="0.0.0.0:8080", help="quine api url. Default '0.0.0.0:8080'")
    parser.add_argument("-c", "--count", type=int, default=10, help="number of values to send. D    efault 10")
    parser.add_argument("-e", "--encodings", type=str, help=f"csv list of encodings from {ENCODINGS}")
    parser.add_argument("-W", "--writeonly", action='store_true',  help="if set will only write to service and not start a quine consumer.")
    parser.add_argument("-R", "--readonly", action='store_true',  help="if set will only start a quine consumer and not write to the service.")
    parser.add_argument("-N", "--testname", type=str, help="Unique test name. Default will be randomly supplied")
    subparsers = parser.add_subparsers(dest="type")
    #
    # kafka args
    #
    kafka_parser = subparsers.add_parser("kafka")
    kafka_parser.add_argument(
        "-k", "--kafka_url", default="localhost:9092", help="kafka url. Default 'localhost:9092'"
    )
    kafka_parser.add_argument("-C", "--commit", default="AutoCommit", help="AutoCommit or ExplicitCommit")
    kafka_parser.add_argument("-t", "--topic", help="kafka topic")
    kafka_parser.add_argument( "--endingoffset", type=int, help="kafka ending offset", required=False)
    kafka_parser.add_argument( "--waitForCommitConfirmation", type=bool, help="kafka ending offset confirmation", required=False, default=True)
    #
    # kinesis args
    #
    kinesis_parser = subparsers.add_parser("kinesis")
    kinesis_parser.add_argument("-n", "--name", help="kinesis stream name", required=True)
    kinesis_parser.add_argument("-r", "--region", help="aws region", default="us-east-1")
    kinesis_parser.add_argument("-k", "--key", help="aws key", required=True)
    kinesis_parser.add_argument("-s", "--secret", help="aws secret", required=True)
    kinesis_parser.add_argument("--checkpoint_batch_size", help="num records before checkpoint. Also requires checkpoint_batch_wait_ms.", required=False)
    kinesis_parser.add_argument("--checkpoint_batch_wait_ms", help="checkpoint batch wait time. Also requires checkpoint_batch_size.", required=False)
    #
    # sqs args
    #
    sqs_parser = subparsers.add_parser("sqs")
    sqs_parser.add_argument("-q", "--queue_url", help="sqs queue url", required=True)
    sqs_parser.add_argument("-r", "--region", help="aws region", default="us-east-1")
    sqs_parser.add_argument("-k", "--key", help="aws key", required=True)
    sqs_parser.add_argument("-s", "--secret", help="aws secret", required=True)

    args = parser.parse_args()
    print(colored(f"ARGS = {args}","blue"))

    testname =   random_string()
    encodings: List[str] = Encoding.parse_csv(args.encodings)
    if args.type == "kafka":
        config = KafkaConfig(testname, args.count, args.quine_url, args.topic, args.kafka_url, args.commit, args.endingoffset, encodings, args.waitForCommitConfirmation)
    elif args.type == "kinesis":
        config = KinesisConfig(testname, args.count, args.quine_url, args.name, encodings, args.checkpoint_batch_size, args.checkpoint_batch_wait_ms,
                               {"region": args.region, "key": args.key, "secret": args.secret})
    elif args.type == "sqs":
        config = SQSConfig(testname, args.count, args.quine_url, args.queue_url, encodings,
                           {"region": args.region, "key": args.key, "secret": args.secret})

    config.run_test(sleep_time_ms=10000,  write=args.writeonly or args.readonly == False, read=args.readonly or args.writeonly == False)

