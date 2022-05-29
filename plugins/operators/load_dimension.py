from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self, redshift_conn_id="", table_name="", sql_query="", *args, **kwargs
    ):
        """Other tables insertion. Replace all table.

        Args:
            redshift_conn_id: Redshift airflow connection id. Defaults to "".
            table_name: table name to truncate and insert in redshift.
            sql_query: Query to execute.
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Truncating table")
        query = "TRUNCATE TABLE {}".format(self.table_name)
        redshift_hook.run(query)
        self.log.info("Inserting records")
        query = "INSERT INTO {} {}".format(self.table_name, self.sql_query)
        redshift_hook.run(query)

