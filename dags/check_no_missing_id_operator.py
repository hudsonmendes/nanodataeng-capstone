from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class CheckNoMissingIdOperator(BaseOperator):
    """
    Checks if all ids in the reference query are present
    in the comparing query, to ensure that the original
    insert worked successfully.
    """

    @apply_defaults
    def __init__(
            self,
            reference_conn_id: str,
            reference_db: str,
            reference_ids_sql: str,
            checking_conn_id: str,
            checking_db: str,
            checking_ids_sql: str,
            *args, **kwargs) -> None:
        """
        Creates the Operator.

        Parameters
        ----------
        reference_conn_id {str} the id of the connection where the `reference_ids_sql` will be run
        reference_db      {str} the database where the `reference_ids_sql` will be run
        reference_ids_sql {str} statement that retrieves all the ids that should be in the destination table
        checking_conn_id  {str} the id of the connection where the `checking_ids_sql` will be run
        checking_db       {str} the database where the `checking_ids_sql` will be run
        checking_ids_sql  {str} statement that retrieves all the ids from the destination table, to compare them.
        """
        super(CheckNoMissingIdOperator, self).__init__(*args, **kwargs)
        self.reference_conn_id = reference_conn_id
        self.reference_db      = reference_db
        self.reference_ids_sql = reference_ids_sql
        self.checking_conn_id  = checking_conn_id
        self.checking_db       = checking_db
        self.checking_ids_sql  = checking_ids_sql

    def execute(self, context):
        """
        Executes the reference query, and collect the ids.
        Executes the checking query, and collect the ids.
        Ensure that all ids in the original query are present in the second.

        Parameters
        ----------
        context {object} Airflow context
        """
        source_hook = CheckNoMissingIdOperator.prepare_hook(self.reference_conn_id, self.reference_db)
        source_ids  = CheckNoMissingIdOperator.collect_ids_from(source_hook, self.reference_ids_sql)
        dest_hook   = CheckNoMissingIdOperator.prepare_hook(self.checking_conn_id, self.checking_db)
        dest_ids    = CheckNoMissingIdOperator.collect_ids_from(dest_hook, self.checking_ids_sql)
        diff = set(source_ids).difference(set(dest_ids))
        print(diff)
        assert len(diff) == 0

    @staticmethod
    def prepare_hook(conn_id, schema):
        """
        Returns the postgres hook, based on the configuration passed into the operator.

        Parameters
        ----------
        conn_id {str} Airflow connection id
        schema  {str} The schema to where the hook will be connected
        """
        return PostgresHook(postgres_conn_id=conn_id, schema=schema)

    @staticmethod
    def collect_ids_from(hook, sql):
        """
        Generates the ids resulting from the sql query.

        Parameters
        ----------
        hook {object} PostgresHook connected to the target database
        sql  {str} statement that should retrieve the column that corresponds to the ID
        """
        cur = hook.get_cursor()
        cur.execute(sql)
        for x in cur.fetchall():
            yield x


    