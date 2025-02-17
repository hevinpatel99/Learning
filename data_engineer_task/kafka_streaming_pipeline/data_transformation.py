from io import StringIO

import pandas as pd
from confluent_kafka import KafkaException, KafkaError

from data_engineer_task.data_validator.sanitize_process import DataSanitization
from data_engineer_task.kafka_streaming_pipeline.config import (
    KAFKA_SERVER,
    VALIDATED_TOPIC, SANITIZED_TOPIC, LOG_FILE_PATH
)
from data_engineer_task.kafka_streaming_pipeline.utils import (
    setup_logger,
    create_kafka_consumer,
    create_kafka_producer, delivery_report,
)

# Setup logger
logger = setup_logger(
    "DataSanitizationConsumer",
    LOG_FILE_PATH + "data_transformation_consumer.log",
)


class DataTransformationProcess:
    """Kafka Consumer to process file messages from Kafka, sanitize, and send to another topic."""

    def __init__(self):
        self.producer = create_kafka_producer(logger)  # Kafka producer instance
        self.consumer = create_kafka_consumer(
            "valid-consumer-group", logger
        )  # Kafka consumer instance
        self.valid_topic = VALIDATED_TOPIC  # Source topic for raw messages consume
        self.sanitized_topic = SANITIZED_TOPIC  # Target topic for validated data produce
        self.sanitized_data_list = []

    def consume_messages(self):
        """Consumes messages from Kafka and processes the file content."""

        idle_count = 0
        idle_threshold = 10  # Stop after 10 consecutive idle polls
        try:
            # Subscribe to the Kafka topic
            self.consumer.subscribe([self.valid_topic])

            logger.info(f"Consumer subscribed to topic: {self.valid_topic}")

            while True:
                # Poll for messages from Kafka
                msg = self.consumer.poll(timeout=1.0)  # Timeout is in seconds

                if msg is None:
                    # Check no message received within the timeout period
                    idle_count += 1
                    logger.info(f"No message received within {idle_count} timeout.")
                    if idle_count >= idle_threshold:
                        logger.info("No new messages detected. Stopping the consumer.")
                        break
                    continue
                elif msg.error():
                    # If there is an error in the message
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition reached
                        logger.info(f"End of partition reached for {msg.topic()} "
                                    f"partition {msg.partition()} at offset {msg.offset()}")
                    else:
                        raise KafkaException(msg.error())
                else:
                    idle_count = 0
                    # Process the message (e.g., print, log, etc.)
                    logger.info(f"Received message from topic: {msg.topic()}, partition: {msg.partition()}, "
                                f"offset: {msg.offset()} with key: {msg.key().decode('utf-8')}")

                    file_name = msg.key().decode('utf-8') if msg.key() else None

                    file_content = msg.value().decode('utf-8') if msg.value() else None

                    # Process the file content
                    self.process_data_sanitization(file_content, file_name)

                    # Commit Kafka offset after successful processing
                    try:
                        self.consumer.commit(asynchronous=False)
                    except KafkaException as e:
                        logger.error(f"Error committing Kafka offset: {e}")

        except Exception as e:
            logger.error(f"An error occurred while consuming messages: {e}", exc_info=True)
            raise
        finally:
            # Close the consumer to release resources
            self.consumer.close()
            logger.info("Consumer closed successfully.")

    def process_data_sanitization(self, file_content, file_name):
        """Sanitizes incoming JSON data"""

        try:
            logger.info(f"Processing file: {file_name} from text to CSV.")

            if not file_content:
                logger.error("Received empty file content.")
                return

            try:
                # Convert JSON string to DataFrame
                data_frame = pd.read_json(StringIO(file_content), lines=True)
            except ValueError as e:
                logger.error(f"Error parsing JSON data: {e}")
                return

            if data_frame.empty:
                logger.warning("Received an empty DataFrame after JSON parsing. Skipping processing.")
                return

            # Convert headers to uppercase
            data_frame.columns = [col.upper() for col in data_frame.columns]

            # Remove duplicate rows
            data_frame.drop_duplicates(inplace=True)

            # Perform data sanitization
            data_sanitization = DataSanitization(data_frame, file_name)
            if not data_sanitization.sanitize_data():
                logger.error(f"Data sanitization failed for file: {file_name}")
                return

            # Convert datetime columns to string format
            for col in data_frame.select_dtypes(include=["datetime64"]):
                data_frame[col] = data_frame[col].dt.strftime("%Y-%m-%d").astype(str)

                # Convert DataFrame to JSON format
            json_data = data_frame.to_json(orient="records",
                                           lines=True)  # JSON format as records (list of dictionaries)

            # Append sanitized data to list
            self.sanitized_data_list.append({"file_name": file_name, "json_data": json_data})

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}", exc_info=True)

    def produce_sanitized_data(self):
        """Produces sanitized data to the Kafka topic."""
        try:
            for record in self.sanitized_data_list:
                try:
                    self.producer.produce(
                        self.sanitized_topic,
                        key=record["file_name"],
                        value=record["json_data"].encode('utf-8'),
                        callback=lambda err, msg: delivery_report(err, msg, logger)
                    )
                except Exception as e:
                    logger.error(f"Error producing record with key {record['file_name']}: {e}", exc_info=True)

                # Ensure all messages are sent
                self.producer.flush()
                logger.info("All records successfully produced to Kafka.")

        except Exception as e:
            logger.error(f"Critical error during message production: {e}", exc_info=True)


if __name__ == "__main__":
    consumer = DataTransformationProcess()
    consumer.consume_messages()
    consumer.produce_sanitized_data()
