import csv
from io import StringIO

import pandas as pd
from confluent_kafka import Consumer, KafkaException, KafkaError

from data_validator.dynamic_data_validation import DataValidation
from kafka_streaming_pipeline.config import KAFKA_SERVER, RAW_TOPIC
from kafka_streaming_pipeline.utils import setup_logger

# Setup logger
logger = setup_logger("FileIngestionConsumer", "file_ingestion_consumer.log")


class FileIngestionConsumer:
    def __init__(self):
        # Create a Kafka Consumer instance
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_SERVER,  # Kafka server address
            'group.id': 'file-ingestion-consumer-group',  # Consumer group ID
            'auto.offset.reset': 'earliest'  # Start consuming from the earliest offset
        })
        self.topic = RAW_TOPIC  # Topic to consume from

    def consume_messages(self):
        try:
            # Subscribe to the Kafka topic
            self.consumer.subscribe([self.topic])

            logger.info(f"Consumer subscribed to topic: {self.topic}")

            # Poll messages from Kafka
            while True:
                # Poll for messages from Kafka
                msg = self.consumer.poll(timeout=1.0)  # Timeout is in seconds

                if msg is None:
                    # No message received within the timeout period
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
                    # Process the message (e.g., print, log, etc.)
                    logger.info(f"Received message from topic: {msg.topic()}, partition: {msg.partition()}, "
                                f"offset: {msg.offset()} with key: {msg.key().decode('utf-8')}")

                    key = msg.key()
                    file_name = key.decode('utf-8') if key else None

                    value = msg.value()
                    input_file_content = value.decode('utf-8') if value else None

                    # logger.info(f"Message value: {msg.value().decode('utf-8')}")
                    print(f"key: {file_name}")
                    self.parsing_text_to_csv_file(input_file_content, file_name)

        except Exception as e:
            logger.error(f"An error occurred while consuming messages: {e}")
            raise
        finally:
            # Close the consumer to release resources
            self.consumer.close()
            logger.info("Consumer closed successfully.")

    @staticmethod
    def parsing_text_to_csv_file(input_file_content, file_name):
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

            json_data = data_frame.to_json(orient="records",
                                           lines=True)  # JSON format as records (list of dictionaries)
            print(f"json : {json_data}")
            # Create the output file name by replacing .txt with .csv
            # output_file_name = file_name.replace('.txt', '.csv')
            # logger.info(f'Output file name: {output_file_name}')
            # return {"file_name": output_file_name, "json_data": validate_data}

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
            return None




# Main function to run the consumer
if __name__ == "__main__":
    consumer = FileIngestionConsumer()
    consumer.consume_messages()
