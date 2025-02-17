"""
Module: utils

Provides utility functions for logging setup, kafka producer/consumer creation,
and message delivery tracking for data kafka streaming pipelines.
"""
import json
import logging

from confluent_kafka import Producer, Consumer

from data_engineer_task.kafka_streaming_pipeline.config import KAFKA_SERVER, DLQ_TOPIC


def setup_logger(name, log_file):
    """
    Sets up and returns a logger with file and console handlers.

    Args:
        name (str): Logger name.
        log_file (str): Log file path.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)

    # File handler for writing logs to a file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Stream handler for logging to the console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Add handlers to the logger
    if not logger.handlers:  # Prevent adding duplicate handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    # Prevent log propagation to the root logger
    logger.propagate = False

    return logger


def create_kafka_producer(logger):
    """
    Creates and returns a Kafka Producer instance.

    Args:
        logger (logging.Logger): Logger instance.

    Returns:
        Producer: Kafka producer instance.
    """
    try:
        producer = Producer({'bootstrap.servers': KAFKA_SERVER})
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logging.error(f"Error creating Kafka Producer: {e}")
        raise


def create_kafka_consumer(group_id, logger, auto_offset_reset='earliest'):
    """
    Creates and returns a Kafka Consumer instance.

    Args:
        group_id (str): Consumer group ID.
        logger: Logger instance.
        auto_offset_reset (str, optional): Offset reset policy ('earliest' or 'latest'). Default is 'earliest'.

    Returns:
        Consumer: Kafka consumer instance.
    """

    try:
        consumer_config = {
            'bootstrap.servers': KAFKA_SERVER,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False
        }

        consumer = Consumer(consumer_config)
        logger.info(f"Kafka Consumer created for group: {group_id}")
        return consumer

    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise


def delivery_report(err, msg, logger):
    """
    Callback function to track Kafka message delivery status.

    Args:
        err (KafkaError or None): Error if message delivery fails.
        msg (Message): Kafka message object.
        logger : Logger instance.
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} partition {msg.partition()}"
                    f"at offset {msg.offset()} with key {msg.key().decode('utf-8')}")

