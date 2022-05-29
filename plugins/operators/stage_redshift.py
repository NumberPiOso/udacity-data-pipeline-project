from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as json {}
        compupdate off
        region 'us-west-2';
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        log_jsonpath="",
        *args,
        **kwargs
    ):
        """Operator that reads JSON in S3 and writes to redshift.

        The operator creates and runs a SQL COPY statement based on
        the parameters provided.
        The operator's parameters  should specify where in S3 the
        file is loaded and what is the target tab

        Args:
            redshift_conn_id: Airflow connection id of redshift. Defaults to "".
            aws_credentials_id: Airflow connection id of aws. Defaults to "".
            table: redshift table name. Defaults to "".
            s3_bucket: s3 bucket input data. Defaults to "".
            s3_key: s3 object input data. Defaults to "".
            log_jsonpath: path to format JSON.
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.log_jsonpath = log_jsonpath

    def execute(self, context):
        """Copy json data from s3 bucket to redshift"""
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_connection = BaseHook().get_connection(self.aws_credentials_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.log_jsonpath,
        )
        redshift.run(formatted_sql)
