import psycopg2
import pandas as pd


class PostgresSqlConnector:
    def __init__(self, connection_credentials={}):
        self.connection_credentials = connection_credentials

    def get_connection(self):
        try:
            connection = psycopg2.connect(
                host=self.connection_credentials.get("host"),
                dbname=self.connection_credentials.get("database"),
                port=self.connection_credentials.get("port"),
                user=self.connection_credentials.get("username"),
                password=self.connection_credentials.get("password"),
            )
        except Exception as e:
            print(e)
            raise e
        return connection

    def execute_sql(self, sql, connection=None):
        if connection is None:
            connection = self.get_connection()
            connection.autocommit = True
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            ##cursor.execute("COMMIT")
            print("Successfully executed {}".format(sql))
        except Exception as e:
            raise e

    def get_execute_sql_result(self, sql, connection=None):
        if connection is None:
            connection = self.get_connection()

        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
        except Exception as e:
            raise e
        else:
            return result

    def query_sql(self, query, connection=None):
        if connection is None:
            connection = self.get_connection()

        return pd.read_sql_query(query, connection)

    def table_exists(self, table_name, schema_name):
        schema_name = schema_name.lower()
        table_name = table_name.lower()
        sql = (
            "select table_name from information_schema.tables "
            f"where table_name = '{table_name}' "
            f"and table_schema = '{schema_name}';"
        )

        df = self.query_sql(query=sql)

        if df.empty:
            return False
        else:
            return True

    def get_column_datatype(self, schema_name, table_name, column_name):
        sql = f"""
            select
                c.data_type
            from information_schema.tables t
                left join information_schema.columns c
                    on t.table_schema = c.table_schema
                    and t.table_name = c.table_name
            where t.table_name = '{table_name}'
            and t.table_schema = '{schema_name}'
            and c.column_name = '{column_name}';
        """
        return self.get_execute_sql_result(sql=sql)[0][0]

    def get_table_primary_keys(self, table_name, schema_name):
        # this is the only method that works in postgres and redshift
        sql = f"""
            SELECT attname column_name
            FROM pg_index ind, pg_class cl, pg_attribute att
            WHERE cl.oid = '{schema_name}.{table_name}'::regclass
            AND ind.indrelid = cl.oid
            AND att.attrelid = cl.oid
            AND att.attnum::text =
                ANY(string_to_array(textin(int2vectorout(ind.indkey)), ' '))
            AND attnum > 0
            AND ind.indisprimary
            ORDER BY att.attnum;
        """

        df = self.query_sql(query=sql)
        return df["column_name"].tolist()
