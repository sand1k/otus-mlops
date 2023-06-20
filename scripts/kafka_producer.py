#!/usr/bin/env python
"""OTUS BigData ML kafka producer example"""

import json
from typing import Dict, NamedTuple
import logging
import random
import datetime
import argparse
from collections import namedtuple

import kafka

MAX_USER_ID = 100
MAX_PAGE_ID = 10


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    offset: int


def main():
    argparser = argparse.ArgumentParser(description=__doc__)
    argparser.add_argument(
        "-b",
        "--bootstrap_server",
        default="rc1a-ck5mbj40cdqjgsn7.mdb.yandexcloud.net:9091",
        help="kafka server address:port",
    )
    argparser.add_argument(
        "-u", "--user", default="mlops", help="kafka user"
    )
    argparser.add_argument(
        "-p", "--password", default="otus-mlops", help="kafka user password"
    )
    argparser.add_argument(
        "-t", "--topic", default="clicks", help="kafka topic to consume"
    )
    argparser.add_argument(
        "-n",
        default=10,
        type=int,
        help="number of messages to send",
    )

    args = argparser.parse_args()

    producer = kafka.KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=args.user,
        sasl_plain_password=args.password,
        ssl_cafile="YandexCA.crt",
        value_serializer=serialize,
    )

    try:
        for i in range(args.n):
            record_md = send_message(producer, args.topic)
            print(
                f"Msg sent. Topic: {record_md.topic}, partition:{record_md.partition}, offset:{record_md.offset}"
            )
    except kafka.errors.KafkaError as err:
        logging.exception(err)
    producer.flush()
    producer.close()


def send_message(producer: kafka.KafkaProducer, topic: str) -> RecordMetadata:
    click = generate_click()
    future = producer.send(
        topic=topic,
        key=str(25).encode("ascii"),
        value=click,
    )

    # Block for 'synchronous' sends
    record_metadata = future.get(timeout=1)
    return RecordMetadata(
        topic=record_metadata.topic,
        partition=record_metadata.partition,
        offset=record_metadata.offset,
    )


def generate_click() -> Dict:
    return {
        # "ts": datetime.datetime.now().isoformat(),
        # "user_id": random.randint(0, MAX_USER_ID),
        # "page_id": random.randint(0, MAX_PAGE_ID),
        "tx_amount": 175.69,
        "is_weekend": 0,
        "is_night": 0,
        "customer_id_nb_tx_1day_window": 2,
        "customer_id_avg_amount_1day_window": 109.625,
        "customer_id_nb_tx_7day_window": 13,
        "customer_id_avg_amount_7day_window": 113.7476923076923,
        "customer_id_nb_tx_30day_window": 53,
        "customer_id_avg_amount_30day_window": 113.7476923076923,
        "terminal_id_nb_tx_1day_window": 68103,
        "terminal_id_risk_1day_window": 0.00177,
        "terminal_id_nb_tx_7day_window": 476562,
        "terminal_id_risk_7day_window": 0.001743739534415,
        "terminal_id_nb_tx_30day_window": 1312174,
        "terminal_id_risk_30day_window": 0.001697183452804
    }


def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode("utf-8")


if __name__ == "__main__":
    main()
