from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", *args, **kwargs):
        """Perform quality checks over data.

        Args:
            redshift_conn_id: Redshioft connection id. Defaults to "".
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """Perform quality checks on data
        
        Checks
        1. Check all star schema table have rows.
        2. Check Columns in tables are not null"""
        self.log.info("Testing not empty tables")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Check
        tables = ["artists", "songplays", "songs", "time", "users"]
        for table in tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results"
                )
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    f"Data quality check failed. {table} contained 0 rows"
                )
            logging.info(
                f"Data quality on table {table} check passed with {records[0][0]} records"
            )

        # Check columns not null
        tbls_cols = [
            ("artists", "artistid"),
            ("songplays", "playid"),
            ("songs", "songid"),
            ("time", "start_time"),
            ("users", "userid"),
        ]
        for table, col in tbls_cols:
            sql_query = f"SELECT COUNT(*) FROM {table} where {col} is NULL"
            records = redshift.get_records(sql_query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results"
                )
            num_records = records[0][0]
            if num_records != 0:
                raise ValueError(
                    f"Data quality check failed. {table} contained {num_records} rows with {col} NULL"
                )
            logging.info(
                f"Data quality on table {table} check passed with no null records in {col}"
            )
