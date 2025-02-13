# Configure logging
import logging

from confluent_kafka import Producer, Consumer


def setup_logger(name, log_file):
    """
    Sets up and returns a logger with the specified name and configuration.

    Args:
        name (str): The name of the logger (e.g., module name).
        log_file (str): Path to the log file.

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


def create_kafka_producer(bootstrap_servers, logger):
    """
    Create and return a Kafka Producer instance.
    """
    try:
        producer = Producer({'bootstrap.servers': bootstrap_servers})
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logging.error(f"Error creating Kafka Producer: {e}")
        raise


def create_kafka_consumer(bootstrap_servers, group_id, logger, auto_offset_reset='earliest'):
    """
        Create and return a Kafka Consumer instance.
        """
    try:
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
        }

        consumer = Consumer(consumer_config)
        logger.info(f"Kafka Consumer created for group: {group_id}")
        return consumer

    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise


def delivery_report(err, msg, logger):
    """Delivery report callback to track message delivery status."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} partition {msg.partition()}"
                    f"at offset {msg.offset()} with key {msg.key().decode('utf-8')}")
