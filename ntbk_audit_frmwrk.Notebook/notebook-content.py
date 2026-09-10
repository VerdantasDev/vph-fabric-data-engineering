# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "24113e54-6f3f-4157-8c30-a3c5e66de623",
# META       "default_lakehouse_name": "VPC_Dev_Fablh_data",
# META       "default_lakehouse_workspace_id": "297572de-b7d7-4285-a88e-1388e2598d4a"
# META     }
# META   }
# META }

# CELL ********************

import os
import logging
from io import StringIO
from datetime import datetime

from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class LoggerSetup:
    def __init__(self, filename):
        """
        Initializes the LoggerSetup instance.
        Args:
            filename (str): The name of the log file.
        """
        self.filename = filename
        self.log_buffer = StringIO()
        self.logger = self.setup_logger()

    def setup_logger(self):
        """
        Sets up the logger with console and file handlers.
        Returns:
            logging.Logger: Configured logger instance.
        """
        logger = logging.getLogger(self.filename)
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        if not logger.handlers:
            # Console Handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

            # File Handler
            file_handler = logging.StreamHandler(self.log_buffer)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        # Disable propagation to avoid duplicate logging from root logger
        logger.propagate = False

        return logger


    def debug(self, message):
        """Logs a debug message."""
        self.logger.debug(message)


    def info(self, message):
        """Logs an info message."""
        self.logger.info(message)


    def warning(self, message):
        """Logs a warning message."""
        self.logger.warning(message)


    def error(self, message):
        """Logs an error message."""
        self.logger.error(message)


    def save_logs(self):
        """
        Saves the logs to a file.
        The logs are saved in a directory structure based on the current date.
        """
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.flush()

        log_data = self.log_buffer.getvalue().encode("utf-8")

        today = datetime.today().date()
        year = today.year
        month = today.strftime("%m")

        root_path = f"/lakehouse/default/Files/Logs/{year}/{month}"
        os.makedirs(root_path, exist_ok=True)

        timestamp = int(datetime.now().timestamp())
        filepath = f"{root_path}/{self.filename}-{timestamp}.log"
        
        with open(filepath, "wb") as file:
            file.write(log_data)


# Example usage:
# logger = LoggerSetup('ntbk_get_contents_from_files')
# logger.debug('This is a debug message.')
# logger.info('This is an info message.')
# logger.save_logs()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_audit_data(df):
    """
    Writes audit data to a Delta table.
    Args:
        df (DataFrame): DataFrame containing the audit data.
    """
    target_table_path = "Tables/delta_table_audit"
    merge_keys = ["TableName", "version"]

    if DeltaTable.isDeltaTable(spark, target_table_path):
        tgt_table = DeltaTable.forPath(spark, target_table_path)

        merge_condition = " and ".join(
            [f" target.{col} = updates.{col} " for col in merge_keys]
        )

        tgt_table.alias("target").merge(
            source=df.alias("updates"), condition=merge_condition
        ).whenMatchedUpdateAll(condition="false").whenNotMatchedInsertAll().execute()

    else:
        df.write.format('delta').mode('append').save(target_table_path)


def save_audit_history(delta_table_path):
    """
    Saves the audit history of a Delta table.
    Args:
        delta_table_path (str): Path to the Delta table.
    """
    try:
        
        tablename = delta_table_path.split("Tables/")[1]
        delta_table = DeltaTable.forPath(spark, delta_table_path)

        history_df = delta_table.history()
        history_df.createOrReplaceTempView("temp")

        query = f"""
            SELECT
                '{tablename}' as TableName,
                version,
                operation,
                readVersion,
                case 
                    when operation = 'MERGE' then concat(
                        operationMetrics['numTargetRowsCopied'], 
                        " - Rows copied, ", 
                        operationMetrics['numTargetRowsCopied'], 
                        " - Rows Inserted, ", 
                        operationMetrics['numTargetRowsCopied'], 
                        " - Rows Updated, "
                    )
                    else concat(
                        operationMetrics['numOutputRows'],
                        " - Rows copied"
                    )
                end as record_count,
                timestamp as load_timestamp
            FROM 
                temp
        """

        df = spark.sql(query)
        write_audit_data(df)
    
    except Exception as e:
        print(e)

# Example usage:
# save_audit_history('Tables/AzureTest')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
