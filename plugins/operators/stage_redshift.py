from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_conn_id="",
                 table = "",
                 s3_path = "",
                 region= "us-west-2",
                 data_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.data_format = data_format
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws = AwsHook(self.aws_conn_id)
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Cleaning destination table")
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info("Copying data from S3 to Redshift")
        # verify for a specific date
        if self.execution_date:
            sql = f"""
                   COPY {self.table} FROM '{self.s3_path}/{self.execution_date.strftime('%Y')}/{self.execution_date.strftime('%d')}/'
                   ACCESS_KEY_ID '{credentials.access_key}'
                   SECRET_ACCESS_KEY '{credentials.secret_key}'
                   REGION '{self.region}'
                   {self.data_format} 'auto';
                   """
        else:
            sql = f"""
                   COPY {self.table} FROM '{self.s3_path}'
                   ACCESS_KEY_ID '{credentials.access_key}'
                   SECRET_ACCESS_KEY '{credentials.secret_key}'
                   REGION '{self.region}'
                   {self.data_format} 'auto';
                   """
        # run sql command
        redshift.run(sql)
