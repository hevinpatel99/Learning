import os
import uuid
from io import StringIO

import pandas as pd
from cassandra.cluster import Cluster
from confluent_kafka import KafkaException, KafkaError

from data_engineer_task.config.casandra_table_config import TABLE_CONFIG
from data_engineer_task.kafka_streaming_pipeline.config import KAFKA_SERVER, SANITIZED_TOPIC
from data_engineer_task.kafka_streaming_pipeline.utils import setup_logger, create_kafka_consumer

logger = setup_logger("DataLoadingProcess",
                      "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/logs_files/data_loading.log")  # Create a logger for this module


# This class use for the insert data into cassandra database
class DataLoadingProcess:

    def __init__(self, keyspace='healthcare_data'):
        self.consumer = create_kafka_consumer(
            KAFKA_SERVER, "valid-consumer-group", logger
        )
        self.cluster = Cluster(['127.0.0.1'], port=9042)  # or ['172.23.0.4']
        self.session = self.cluster.connect()
        self.create_keyspace_if_not_exists(keyspace)
        self.session.set_keyspace(keyspace)
        self.sanitized_topic = SANITIZED_TOPIC

    def create_keyspace_if_not_exists(self, keyspace):
        try:
            # Check if the keyspace exists by running a query (this will fail if the keyspace doesn't exist)
            self.session.execute(f"USE {keyspace}")
        except Exception as e:
            logger.info(f"Critical error during message production: {e}")
            # If it doesn't exist, catch the exception and create the keyspace
            print(f"Keyspace '{keyspace}' does not exist. Creating it now...")
            self.create_keyspace(keyspace)
        pass

    def create_keyspace(self, keyspace):
        """Create the keyspace with NetworkTopologyStrategy (for multi-node clusters)."""
        try:
            create_keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}};
            """
            self.session.execute(create_keyspace_query)

            print(f"Keyspace '{keyspace}' created successfully.")
        except Exception as e:
            print(f"Error creating keyspace '{keyspace}': {e}")

    def consume_messages(self):
        idle_count = 0
        idle_threshold = 10  # Stop after 10 consecutive idle polls
        try:
            # Subscribe to the Kafka topic
            self.consumer.subscribe([self.sanitized_topic])

            logger.info(f"Consumer subscribed to topic: {self.sanitized_topic}")

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
                    self.process_data_insertion(input_file_content, file_name)

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

    def process_data_insertion(self, input_file_content, file_name):
        try:
            logger.info(f"Processing file: {file_name} from text to CSV.")

            if not input_file_content:
                logger.error("Received empty file content.")
                return

            try:
                data_frame = pd.read_json(StringIO(input_file_content), lines=True)
            except ValueError as e:
                logger.error(f"Error parsing  data: {e}")
                return

            if data_frame.empty:
                logger.warning("Received an empty DataFrame after  parsing. Skipping processing.")
                return

            # Ensure column names are in uppercase
            data_frame.columns = [col.upper() for col in data_frame.columns]

            data_frame.rename(columns={"Q": "QUANTITY"}, inplace=True)

            date_columns = ['DOB', 'START_DATE', 'END_DATE', 'DATE', 'DATE_OF_BIRTH', 'DATE_OF_DEATH', 'TIMESTAMP']

            # Check if any column from 'date_columns' exists in the DataFrame
            for col in date_columns:
                if col in data_frame.columns:
                    data_frame[col] = pd.to_datetime(data_frame[col], errors="coerce")  # Convert to datetime
                    data_frame[col] = data_frame[col].apply(
                        lambda x: None if pd.isna(x) else x.strftime("%Y-%m-%d"))  # Handle NaN and format

            self.insert_data_in_storage(file_name, data_frame)

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")

    def insert_data_in_storage(self, file_name, data_frame):

        name = os.path.splitext(file_name)[0]

        if name == "allergies":
            table_name = "patient_allergies"
            self.process_data(data_frame, table_name)
        elif name == "patients":
            table_name = "patient_info"
            self.process_data(data_frame, table_name)
        elif name == "familyhistory":
            table_name = "patient_family_history"
            self.process_data(data_frame, table_name)
        elif name == "problems":
            table_name = "patient_diagnoses"
            self.process_data(data_frame, table_name)
        elif name == "procedures":
            table_name = "patient_procedure"
            self.process_data(data_frame, table_name)
        elif name == "refills":
            table_name = "patient_refills"
            self.process_data(data_frame, table_name)
        elif name == "labs":
            table_name = "patient_labs"
            self.process_data(data_frame, table_name)
        elif name == "meds":
            table_name = "patient_meds"
            self.process_data(data_frame, table_name)
        elif name == "vitals":
            table_name = "patient_vitals"
            self.process_data(data_frame, table_name)
        elif name == "socialhistory":
            table_name = "patient_social_history"
            self.process_data(data_frame, table_name)
        else:
            print(f"No file found...")

    def process_data(self, data_frame, table_name):
        config = TABLE_CONFIG.get(table_name, {})
        self.create_table_dynamically(config, table_name)

        # ✅ Iterate through DataFrame rows and insert each row as a dictionary
        if isinstance(data_frame, pd.DataFrame):
            for _, row in data_frame.iterrows():
                self.insert_data_into_table(table_name, row.to_dict())  # ✅ Convert row to dict
        elif isinstance(data_frame, dict):
            self.insert_data_into_table(table_name, data_frame)  # ✅ Already a dict
        else:
            print(f"Invalid data type for '{table_name}': {type(data_frame)}")

    def create_table_dynamically(self, config, table_name):
        """Creates a table dynamically from a  config file."""

        if not config:
            print(f"No configuration found for table '{table_name}'")
            return

        columns_query = ", ".join([f"{col} {dtype}" for col, dtype in config.items()])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_query});"

        # Execute query
        self.session.execute(create_table_query)
        print(f"Table '{table_name}' created successfully!")

        # Execute query
        self.session.execute(create_table_query)
        print(f"Table '{table_name}' created successfully!")

    def insert_data_into_table(self, table_name, data):
        """
        Insert data dynamically into a Cassandra table.
        :param table_name: Name of the table
        :param data: Dictionary containing column-value pairs
        """
        config = TABLE_CONFIG.get(table_name)
        if not config:
            print(f"No configuration found for table '{table_name}'")
            return

        # Ensure 'ID' is present; if not, generate a unique UUID

        if 'ID' in config:
            if "ID" not in data:
                data["ID"] = uuid.uuid4()  # Generates a unique ID

        # Extract columns and values from the dictionary
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        values = tuple(data.values())

        # Construct INSERT query dynamically
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"

        # Execute query
        self.session.execute(insert_query, values)
        print(f"Data inserted into '{table_name}': {data}")


if __name__ == "__main__":
    consumer = DataLoadingProcess()
    consumer.consume_messages()
