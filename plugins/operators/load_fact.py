from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self, redshift_conn_id="", table_name="", sql_query="", *args, **kwargs
    ):
        """Songplays insert

        Args:
            redshift_conn_id: Redshift airflow connection id. Defaults to "".
            table_name: table name to truncate and insert in redshift
            sql_query: Query to execute.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query

    def execute(self, context):
        """Execute SqlQueries.songplay_table_insert

        Args:
            context: airflow context
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Running songplay deletes")
        query = "TRUNCATE TABLE {}".format(self.table_name)
        redshift.run(query)
        self.log.info("Run songplay inserts")
        query = "INSERT INTO {} {}".format(self.table_name, self.sql_query)
        redshift.run(query)
