from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import operator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.checks = checks
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)    
        for table in self.tables:
            self.log.info(f"Analyzing Table {table}.")
            for check in self.checks:
                records = redshift.get_records(check['test_sql'].format(table=table))
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed on table '{table}', returned no results")
                if not eval(f"{records[0][0]} {check['comparison']} {check['expected_result']}"):
                    raise ValueError(f"Data quality check failed on table '{table}'")
                self.log.info(f"Data quality check passed on table {table}.")