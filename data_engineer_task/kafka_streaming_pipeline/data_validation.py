import csv
from io import StringIO

import pandas as pd
from confluent_kafka import KafkaException, KafkaError

from data_engineer_task.data_validator.validation_process import DataValidation
from data_engineer_task.kafka_streaming_pipeline.config import KAFKA_SERVER, RAW_TOPIC, VALIDATED_TOPIC, LOG_FILE_PATH
from data_engineer_task.kafka_streaming_pipeline.utils import setup_logger, create_kafka_producer, delivery_report, \
    create_kafka_consumer, send_to_dlq

# Setup logger
logger = setup_logger("FileIngestionConsumer",
                      LOG_FILE_PATH + "file_ingestion_consumer.log")


class FileIngestionConsumer:
    """Kafka Consumer to process file messages from Kafka, validate, and send to another topic."""

    def __init__(self):

        self.producer = create_kafka_producer(logger)  # Kafka producer instance
        self.consumer = create_kafka_consumer('file-ingestion-consumer-group',
                                              logger)  # Kafka consumer instance
        self.topic = RAW_TOPIC  # Source topic for raw messages consume
        self.validated_topic = VALIDATED_TOPIC  # Target topic for validated data produce
        self.validated_data_list = []

    def consume_messages(self):
        """Consumes messages from Kafka and processes the file content."""

        idle_count = 0
        idle_threshold = 10  # Stop consuming after 10 consecutive idle polls
        try:
            # Subscribe to the Kafka topic for consuming message
            self.consumer.subscribe([self.topic])
            logger.info(f"Consumer subscribed to topic: {self.topic}")

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
                    idle_count = 0  # Reset idle counter on successful message receiving
                    logger.info(f"Received message from topic: {msg.topic()}, partition: {msg.partition()}, "
                                f"offset: {msg.offset()} with key: {msg.key().decode('utf-8')}")

                    file_name = msg.key().decode('utf-8') if msg.key() else None

                    file_content = msg.value().decode('utf-8') if msg.value() else None

                    # Process the file and produce validated data
                    self.process_file_content(file_content, file_name)

                    # Commit Kafka offset after successful processing
                    try:
                        self.consumer.commit(asynchronous=False)
                    except KafkaException as e:
                        logger.error(f"Error committing Kafka offset: {e}")

        except Exception as e:
            logger.error(f"An error occurred while consuming messages: {e}", exc_info=True)

        finally:
            # Close the consumer to release resources
            self.consumer.close()
            logger.info("Consumer closed successfully.")

    def process_file_content(self, file_content, file_name):
        """Parses text file content and validates it before producing to Kafka."""

        try:
            logger.info(f"Parsing file: {file_name}")

            if not file_name.endswith('.txt'):
                raise ValueError(f"Unsupported file type: {file_name}")

            # handle delimiter
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(file_content.splitlines()[0])
            delimiter = dialect.delimiter
            print(f"Detected delimiter: '{delimiter}'")

            if delimiter != '\t':
                logger.warning(f"Detected delimiter is not tab: '{delimiter}', converting to '\\t'")
                delimiter = '\t'  # Manually set the delimiter to tab

            # Convert text content into DataFrame
            data_frame = pd.read_csv(StringIO(file_content), sep=delimiter, dtype=str)  # Adjusted for CSV format
            logger.info(
                f"Successfully parsed {file_name} with {data_frame.shape[0]} rows and {data_frame.shape[0]} columns")

            # Drop unwanted columns
            columns_to_drop = [col for col in ['GENDER', 'TIMESTAMP'] if col in data_frame.columns]
            if columns_to_drop:
                data_frame = data_frame.drop(columns=columns_to_drop)

            # Convert headers to uppercase
            data_frame.columns = [col.upper() for col in data_frame.columns]

            # Capitalize first letter of headers
            # df.columns = [col.capitalize() for col in df.columns]

            # Perform Validate data
            data_validation = DataValidation(data_frame, file_name)

            if not data_validation.validate_data():
                logger.error(f"Data validation failed for file: {file_name}, Skipping message production.")
                return

            # Convert to JSON format as records (list of dictionaries)
            json_data = data_frame.to_json(orient="records",
                                           lines=True)

            output_file_name = file_name.replace('.txt', '.json').encode('utf-8')
            logger.info(f"Output filename : {output_file_name}")

            # Store validated data for later Kafka production.
            self.validated_data_list.append({"file_name": output_file_name, "json_data": json_data})

            logger.info(f"Validated file: {file_name}, prepared for Kafka production.")

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}", exc_info=True)
            return None

    def produce_validated_data(self):
        """Produces validated JSON data to Kafka."""
        try:
            for record in self.validated_data_list:
                try:
                    self.producer.produce(
                        self.validated_topic,
                        key=record["file_name"],
                        value=record["json_data"].encode('utf-8'),
                        callback=lambda err, msg: delivery_report(err, msg, logger)
                    )
                except Exception as e:
                    logger.error(f"Error producing record with key {record['file_name']}: {e}", exc_info=True)

                # Ensure all messages are sent
                self.producer.flush()
                logger.info("All validated records successfully produced to Kafka.")

        except Exception as e:
            logger.error(f"Critical error during message production: {e}", exc_info=True)


# Main function to run the consumer
if __name__ == "__main__":
    consumer = FileIngestionConsumer()
    consumer.consume_messages()
    consumer.produce_validated_data()
