import os
import time
from confluent_kafka import KafkaException
from data_engineer_task.kafka_streaming_pipeline.utils import setup_logger, create_kafka_producer

from data_engineer_task.kafka_streaming_pipeline.config import KAFKA_SERVER, RAW_TOPIC, INPUT_DIR, LOG_FILE_PATH

from data_engineer_task.kafka_streaming_pipeline.utils import delivery_report

# setup logger
logger = setup_logger("FileIngestionPipeline",
                      LOG_FILE_PATH + "file_ingestion.log")  # Create a logger for this module


class FileIngestionPipeline:
    """Handles reading files from a directory and publishing their content to Kafka."""

    def __init__(self, input_dir):
        self.input_dir = input_dir  # Directory containing files
        self.producer = create_kafka_producer(logger)  # Kafka producer instance
        self.topic = RAW_TOPIC  # Kafka topic to publish messages

    # Method to iterate over the files in the provided directory
    def ingest_files_from_directory(self):
        """Fetches all .txt files from the input directory."""
        list_of_input_files = []
        try:

            for file_name in os.listdir(self.input_dir):
                # Iterate over each file and check if it ends with a '.txt' extension
                if file_name.endswith('.txt'):  # Process only .txt files
                    # Build the full path to the current file by joining the directory and file name
                    input_file_path = os.path.join(self.input_dir, file_name)
                    if input_file_path:
                        list_of_input_files.append(input_file_path)
                else:
                    logger.debug(f"Unsupported file type: {file_name}")

            logger.info(f"Total files found: {len(list_of_input_files)}")
        except Exception as ex:
            logger.error(f"Error while listing files in the directory: {ex}", exc_info=True)
            raise
        return list_of_input_files

    def send_file_content_as_messages(self):
        """Reads each file and sends its content to Kafka."""

        try:
            # Read files from directory
            files = self.ingest_files_from_directory()
            if not files:
                logger.warning("No valid files found for processing.")
                return

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

                    file_name = os.path.basename(file_path)  # Filename as Kafka message key
                    key = file_name.encode('utf-8')  # Kafka expects the key as bytes

                    # Send message to Kafka
                    try:
                        self.producer.produce(
                            self.topic,
                            key=key,
                            value=file_content,
                            callback=lambda err, msg: delivery_report(err, msg, logger)
                        )
                        logger.info(f"File '{file_path}' sent to Kafka topic '{self.topic}'")

                    except KafkaException as ke:
                        logger.error(f"Failed to send message for file {file_name}: {ke}", exc_info=True)
                        continue  # Skip producing message for this file

                    time.sleep(1)

                except Exception as ex:
                    logger.error(f"Error processing file {file_path}: {ex}", exc_info=True)
                    continue

            # Ensure all messages are sent before exiting
            self.producer.flush()
            logger.info("All messages sent successfully.")

        except KafkaException as ke:
            logger.error(f"Kafka error occurred: {ke}", exc_info=True)
            raise

        except Exception as ex:
            logger.error(f"An error occurred while sending messages: {ex}", exc_info=True)
            raise


if __name__ == "__main__":
    try:
        # Ensure the input directory exists
        if not os.path.isdir(INPUT_DIR):
            raise ValueError(f"Input directory does not exist: {INPUT_DIR}")

        # Create an instance of the FileIngestionPipeline class
        raw_producer = FileIngestionPipeline(INPUT_DIR)
        # Start the file ingestion and message sending process
        raw_producer.send_file_content_as_messages()

    except Exception as ex:
        logger.critical(f"Fatal error in file ingestion pipeline: {ex}", exc_info=True)
        raise
