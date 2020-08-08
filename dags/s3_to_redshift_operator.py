from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    """
    Run a COPY command in Redshift targeting a S3 file or folder.
    """

    @apply_defaults
    def __init__(
            self,
            redshift_destination_conn_id: str,
            redshift_destination_db: str,
            redshift_destination_table_name: str,
            redshift_destination_table_columns: dict,
            s3_source_bucket: str,
            s3_source_path: str,
            iam_role: str,
            region: str,
            copy_hints: list,
            *args, **kwargs) -> None:
        """
        Creates the Operator.

        Parameters
        ----------
        redshift_destination_conn_id       {str}  Airflow connection id
        redshift_destination_db            {str}  The name of database of the Data Warehouse
        redshift_destination_table_name    {str}  The table into which data will be copied
        redshift_destination_table_columns {dict} The colums structure of the table.
        s3_source_bucket                   {str}  The S3 bucket from which data will be copied
        s3_source_path                     {str}  The S3 key (folder or file) from which data will be copied
        iam_role                           {str}  The IAM role that can perform the copy from S3 to Redshift
        region                             {str}  The region where both the S3 bucket and the Redshift instance are
        copy_hints                         {list} The copy hints, telling the format, compression, schemas, etc.
        """
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_destination_conn_id = redshift_destination_conn_id
        self.redshift_destination_db = redshift_destination_db
        self.redshift_destination_table_name = redshift_destination_table_name
        self.redshift_destination_table_columns = redshift_destination_table_columns
        self.s3_source_bucket = s3_source_bucket
        self.s3_source_path = s3_source_path
        self.iam_role = iam_role
        self.region = region
        self.copy_hints = copy_hints

    def execute(self, context):
        """
        Prepare the SQL statement, replacing variables and run the command.

        Parameters
        ----------
        context {object} Airflow context
        """
        hook = self.prepare_hook()
        sql = self.prepare_sql()
        hook.run(sql, autocommit=True)
        for output in hook.conn.notices:
            self.log.info(output)

    def prepare_hook(self):
        """
        Returns the postgres hook, based on the configuration passed into the operator.
        """
        return PostgresHook(
            postgres_conn_id=self.redshift_destination_conn_id,
            schema=self.redshift_destination_db)

    def prepare_sql(self):
        """
        Prepares the SQL statement using the parameters provided to the Operator.
        """
        columns = self.redshift_destination_table_columns
        sql = []
        sql.append(f'DROP TABLE IF EXISTS {self.redshift_destination_table_name};')
        sql.append(f'CREATE TABLE {self.redshift_destination_table_name} (')
        for i, (name, definition) in enumerate(columns.items()):
            not_last = i != len(columns) - 1
            ending = ',' if not_last else ''
            sql.append(f'{name} {definition}{ending}')
        sql.append(');')
        sql.append(f'COPY {self.redshift_destination_table_name}')
        sql.append(f"FROM 's3://{self.s3_source_bucket}/{self.s3_source_path}'")
        sql.append(f"IAM_ROLE '{self.iam_role}'")
        sql.append(' '.join(self.copy_hints) + ';')
        return '\n'.join(sql)
