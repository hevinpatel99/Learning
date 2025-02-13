from io import StringIO

import pandas as pd
from confluent_kafka import KafkaException, KafkaError

from data_engineer_task.data_validator.sanitize_process import DataSanitization
from data_engineer_task.kafka_streaming_pipeline.config import (
    KAFKA_SERVER,
    VALIDATED_TOPIC, SANITIZED_TOPIC
)
from data_engineer_task.kafka_streaming_pipeline.utils import (
    setup_logger,
    create_kafka_consumer,
    create_kafka_producer, delivery_report,
)

logger = setup_logger(
    "DataSanitizationConsumer",
    "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/logs_files/data_transformation_consumer.log",
)


class DataTransformationProcess:
    def __init__(self):
        self.producer = create_kafka_producer(KAFKA_SERVER, logger)
        self.consumer = create_kafka_consumer(
            KAFKA_SERVER, "valid-consumer-group", logger
        )
        self.valid_topic = VALIDATED_TOPIC
        self.sanitized_topic = SANITIZED_TOPIC
        self.sanitized_data_list = []

    def consume_messages(self):
        idle_count = 0
        idle_threshold = 10  # Stop after 10 consecutive idle polls
        try:
            # Subscribe to the Kafka topic
            self.consumer.subscribe([self.valid_topic])

            logger.info(f"Consumer subscribed to topic: {self.valid_topic}")

            # Poll messages from Kafka
            while True:
                # Poll for messages from Kafka
                msg = self.consumer.poll(timeout=1.0)  # Timeout is in seconds

                if msg is None:
                    print(idle_count)
                    # No message received within the timeout period
                    idle_count += 1
                    logger.info("No message received within timeout.")
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

                    key = msg.key()
                    file_name = key.decode("utf-8") if key else None

                    # file_name = key.decode('utf-8') if key else None

                    value = msg.value()
                    input_file_content = value.decode("utf-8") if value else None

                    print(f"file content : {type(input_file_content)} || {input_file_content}")

                    # Process the file content
                    self.process_data_sanitization(input_file_content, file_name)

                    try:
                        self.consumer.commit(asynchronous=False)
                    except KafkaException as e:
                        logger.error(f"Error committing Kafka offset: {e}")

        except Exception as e:
            logger.error(f"An error occurred while consuming messages: {e}")
            raise
        finally:
            # Close the consumer to release resources
            self.consumer.close()
            logger.info("Consumer closed successfully.")

    def process_data_sanitization(self, input_file_content, file_name):
        try:
            logger.info(f"Processing file: {file_name} from text to CSV.")

            if not input_file_content:
                logger.error("Received empty file content.")
                return

            try:
                data_frame = pd.read_json(StringIO(input_file_content), lines=True)
            except ValueError as e:
                logger.error(f"Error parsing JSON data: {e}")
                return

            if data_frame.empty:
                logger.warning("Received an empty DataFrame after JSON parsing. Skipping processing.")
                return

            # Ensure column names are in uppercase
            data_frame.columns = [col.upper() for col in data_frame.columns]

            # Remove duplicate rows
            data_frame.drop_duplicates(inplace=True)

            # Perform data sanitization
            data_sanitization = DataSanitization(data_frame, file_name)
            if not data_sanitization.sanitize_data():
                logger.error(f"Data sanitization failed for file: {file_name}")
                return

            for col in data_frame.select_dtypes(include=["datetime64"]):
                data_frame[col] = data_frame[col].dt.strftime("%Y-%m-%d").astype(str)  # C

            json_data = data_frame.to_json(orient="records",
                                           lines=True)  # JSON format as records (list of dictionaries)

            output_data = {"file_name": file_name, "json_data": json_data}

            self.sanitized_data_list.append(output_data)

            # Convert to JSON format for further processing


        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")

    def produce_sanitized_data(self):
        print(f"Produce the validated data")
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
                    logger.error(f"Error producing record with key {record['file_name']}: {e}")

                # Ensure all messages are sent
                self.producer.flush()
                logger.info("All records successfully produced to Kafka.")

        except Exception as e:
            logger.error(f"Critical error during message production: {e}")


if __name__ == "__main__":
    consumer = DataTransformationProcess()
    consumer.consume_messages()
    consumer.produce_sanitized_data()
