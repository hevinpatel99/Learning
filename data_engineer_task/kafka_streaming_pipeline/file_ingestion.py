import os
import time
from confluent_kafka import  KafkaException
from data_engineer_task.kafka_streaming_pipeline.utils import setup_logger, create_kafka_producer

from data_engineer_task.kafka_streaming_pipeline.config import KAFKA_SERVER, RAW_TOPIC, INPUT_DIR

from data_engineer_task.kafka_streaming_pipeline.utils import delivery_report

logger = setup_logger("FileIngestionPipeline",
                      "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/logs_files/file_ingestion.log")  # Create a logger for this module


class FileIngestionPipeline:
    def __init__(self, input_dir):
        # Constructor to initialize the object with the input directory path
        self.input_dir = input_dir  # Instance attribute
        self.producer = create_kafka_producer(KAFKA_SERVER, logger)
        self.topic = RAW_TOPIC

    # Method to iterate over the files in the provided directory
    def ingest_files_from_directory(self):
        list_of_input_files = []
        try:

            for file_name in os.listdir(self.input_dir):
                # Iterate over each file and check if it ends with a '.txt' extension
                if file_name.endswith('.txt'):  # Only process .txt files
                    # Build the full path to the current file by joining the directory and file name
                    input_file_path = os.path.join(self.input_dir, file_name)
                    if input_file_path:
                        list_of_input_files.append(input_file_path)
                        # Print the full path of the current .txt file
                else:
                    print(f"Unsupported file type: {file_name}")

            logger.info(f"list of input file path : {list_of_input_files}")
        except Exception as e:
            logger.error(f"Error while listing files in the directory: {e}")
            raise
        return list_of_input_files

    def send_file_content_as_messages(self):

        try:
            # Read files from directory
            files = self.ingest_files_from_directory()
            logger.info(f"Found {len(files)} file(s) to process.")

            for file_path in files:
                try:
                    # Open and read the file to check if it's empty or contains only whitespace
                    with open(file_path, 'r', encoding='utf-8') as file:
                        file_content = file.read()

                    # If content is empty or contains only whitespace, skip processing
                    if not file_content.strip():
                        logger.warning(f"File {file_path} is empty or contains only whitespace. Skipping...")
                        continue

                    # Extract the filename to use as the key
                    file_name = os.path.basename(file_path)  # For Linux/Unix-like systems
                    key = file_name.encode('utf-8')  # Kafka expects the key as bytes

                    # Send the file content with the filename as the key
                    # Try sending the file content to Kafka
                    try:
                        self.producer.produce(
                            self.topic,
                            key=key,
                            value=file_content,
                            callback=lambda err, msg: delivery_report(err, msg, logger)
                        )
                        logger.info(f"Message sent from file: {file_name}")
                    except KafkaException as ke:
                        logger.error(f"Failed to send message for file {file_name}: {ke}")
                        continue  # Skip producing message for this file

                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
                    continue

            # Wait for any outstanding messages to be delivered and delivery reports
            self.producer.flush()
            logger.info("All messages sent successfully.")

        except KafkaException as ke:
            logger.error(f"Kafka error occurred: {ke}")
            raise

        except Exception as e:
            logger.error(f"An error occurred while sending messages: {e}")
            raise

    # @staticmethod
    # def delivery_report(err, msg):
    #     """Delivery report callback to track message delivery status."""
    #     if err is not None:
    #         logger.error(f"Message delivery failed: {err}")
    #     else:
    #         logger.info(f"Message delivered to {msg.topic()} partition {msg.partition()}"
    #                     f"at offset {msg.offset()} with key {msg.key().decode('utf-8')}")


if __name__ == "__main__":
    try:
        # Ensure the input directory exists
        if not os.path.isdir(INPUT_DIR):
            raise ValueError(f"Input directory does not exist: {INPUT_DIR}")

        # Create an instance of the FileIngestionPipeline class
        raw_producer = FileIngestionPipeline(INPUT_DIR)
        # Start the file ingestion and message sending process
        raw_producer.send_file_content_as_messages()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
