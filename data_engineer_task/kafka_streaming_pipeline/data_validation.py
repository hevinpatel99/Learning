import csv
import time
from io import StringIO

import pandas as pd
from confluent_kafka import Consumer, KafkaException, KafkaError

from data_engineer_task.data_validator.dynamic_data_validation import DataValidation
from data_engineer_task.kafka_streaming_pipeline.config import KAFKA_SERVER, RAW_TOPIC, VALIDATED_TOPIC
from data_engineer_task.kafka_streaming_pipeline.utils import setup_logger, create_kafka_producer, delivery_report

# Setup logger
logger = setup_logger("FileIngestionConsumer",
                      "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/logs_files/file_ingestion_consumer.log")


class FileIngestionConsumer:
    def __init__(self):
        # Create a Kafka Consumer instance
        self.producer = create_kafka_producer(KAFKA_SERVER)
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_SERVER,  # Kafka server address
            'group.id': 'file-ingestion-consumer-group',  # Consumer group ID
            'auto.offset.reset': 'earliest'  # Start consuming from the earliest offset
        })
        self.topic = RAW_TOPIC  # Topic to consume from
        self.out_topic = VALIDATED_TOPIC

    def consume_messages(self):

        idle_count = 0
        idle_threshold = 10  # Stop after 10 consecutive idle polls
        try:
            # Subscribe to the Kafka topic
            self.consumer.subscribe([self.topic])

            logger.info(f"Consumer subscribed to topic: {self.topic}")

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
                    file_name = key.decode('utf-8') if key else None

                    value = msg.value()
                    input_file_content = value.decode('utf-8') if value else None

                    # Process the file and produce validated data
                    self.parsing_text_to_csv_file(input_file_content, file_name)

                    time.sleep(1)

                    # logger.info(f"Message value: {msg.value().decode('utf-8')}")
                    print(f"key: {file_name}")



        except Exception as e:
            logger.error(f"An error occurred while consuming messages: {e}")
            raise
        finally:
            # Close the consumer to release resources
            self.consumer.close()
            logger.info("Consumer closed successfully.")

    # @staticmethod
    def parsing_text_to_csv_file(self, input_file_content, file_name):
        try:
            logger.info(f"Processing file: {file_name} from text to CSV.")

            if not file_name.endswith('.txt'):
                raise ValueError(f"Unsupported file type: {file_name}")

            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(input_file_content.splitlines()[0])
            delimiter = dialect.delimiter
            print(f"Detected delimiter: '{delimiter}'")

            if delimiter != '\t':
                logger.warning(f"Detected delimiter is not tab: '{delimiter}', converting to '\\t'")
                delimiter = '\t'  # Manually set the delimiter to tab

            # Load the text content into a pandas DataFrame (assuming tab-separated text)
            data_frame = pd.read_csv(StringIO(input_file_content), sep=delimiter, dtype=str)  # Adjusted for CSV format
            logger.info(f"Processed file content from {file_name} successfully.")

            if 'GENDER' in data_frame:
                data_frame = data_frame.drop(columns=["GENDER"])

            # Capitalize first letter of headers
            data_frame.columns = [col.upper() for col in data_frame.columns]  # Convert headers to uppercase

            # Capitalize first letter of headers
            # df.columns = [col.capitalize() for col in df.columns]

            # print(f"Data : {data_frame}")
            print(f"self.df.columns : {data_frame.columns}")

            # 4. Validate the data in the DataFrame before converting to CSV
            data_validation = DataValidation(data_frame, file_name)
            if not data_validation.validate_data():
                logger.error(f"Data validation failed for file: {file_name}")
                return

            # Convert the DataFrame to a CSV string (without index)
            # csv_data = df.to_csv(index=False)

            output_file_name = file_name.replace('.txt', '.json').encode('utf-8')

            json_data = data_frame.to_json(orient="records",
                                           lines=True)  # JSON format as records (list of dictionaries)

            logger.info(f"Output filename : {output_file_name}")

            self.produce_validated_data(output_file_name, json_data)

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
            return None

    def produce_validated_data(self, output_file_name, json_data):
        try:
            logger.info(f"Producing validated data for file: {output_file_name} to topic: {self.out_topic}")

            # Send as single record or list of JSON

            self.producer.produce(
                self.out_topic,
                key=output_file_name,
                value=str(json_data).encode('utf-8')
            )

            self.producer.flush()
            logger.info(f"Data successfully produced to topic: {self.out_topic} for file: {output_file_name}")

        except Exception as e:
            logger.error(f"Error producing validated data for file {output_file_name}: {e}")


# Main function to run the consumer
if __name__ == "__main__":
    consumer = FileIngestionConsumer()
    consumer.consume_messages()
