from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        self.log.info("Connect to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("run insert query to load data")
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))
