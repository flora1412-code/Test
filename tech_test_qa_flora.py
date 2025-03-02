import yaml
from postgres_connector import PostgresSqlConnector


class DataValidator:
    def __init__(self, db_credentials: dict, validation_config_path: str) -> None:
        self.validation_config = validation_config_path
        self.validation_config = self.read_validation_config()
        self.db_connection = PostgresSqlConnector(connection_credentials=db_credentials)

    def read_validation_config(self):
        with open(self.validation_config, "r") as f:
            return yaml.full_load(f)

    def validate_columns(self):
        for table_name, validations in self.validation_config["qa_validations"].items():
            if validations.get("validate_columns"):
                print(f"start data validation for {table_name}")
                for column_validation in validations["validate_columns"]:
                    for column_name, validations in column_validation.items():
                        for validation_type, value in validations.items():
                            if validation_type == "data_type":
                                column_datatype = (
                                    self.db_connection.get_column_datatype(
                                        schema_name=table_name.split(".")[0],
                                        table_name=table_name.split(".")[1],
                                        column_name=column_name,
                                    )
                                )
                                if column_datatype != value:
                                    raise Exception(
                                        f"Data type validation failure for column:{column_name}: expected type is {value} but get {column_datatype}"
                                    )
                                else:
                                    print(
                                        f"Datatype validation is passed for column {column_name}!"
                                    )
                            if validation_type == "is_null" and value is False:
                                validation_query = f"""select 1 from {table_name} where {column_name} is NULL"""
                                if not self.db_connection.query_sql(
                                    query=validation_query
                                ).empty:
                                    raise Exception(
                                        f"Data is_null validation failure for column: {column_name}!"
                                    )
                                else:
                                    print(
                                        f"Data is_null validation passed for column {column_name}!"
                                    )
                            if validation_type == "is_unique" and value is True:
                                validation_query = f"""select {column_name}, count(*) from {table_name} group by 1 having count(*)>1"""
                                if not self.db_connection.query_sql(
                                    query=validation_query
                                ).empty:
                                    raise Exception(
                                        f"Data is_unique validation failure for column: {column_name}!"
                                    )
                                else:
                                    print(
                                        f"Data is_unique validation passed for column {column_name}!"
                                    )
                            if validation_type == "allowed_values":
                                validation_query = f"""select * from {table_name} where {column_name} not in ({','.join([str(x) for x in value])}) """
                                if not self.db_connection.query_sql(
                                    query=validation_query
                                ).empty:
                                    raise Exception(
                                        f"Data allowed_values validation failure for column: {column_name}!"
                                    )
                                else:
                                    print(
                                        f"Data allowed_values validation passed for column {column_name}!"
                                    )
                            if validation_type == "min_value":
                                validation_query = f"""select * from {table_name} where {column_name} < {value} """
                                if not self.db_connection.query_sql(
                                    query=validation_query
                                ).empty:
                                    raise Exception(
                                        f"Data min_value validation failure for column: {column_name}!"
                                    )
                                else:
                                    print(
                                        f"Data min_value validation passed for column {column_name}!"
                                    )

    def validate_custom_query(self):
        for table_name, validations in self.validation_config["qa_validations"].items():
            if validations.get("custom_validation"):
                print(f"start custom_validation for {table_name}")
                for custom_valdiation in validations.get("custom_validation"):
                    if not self.db_connection.query_sql(query=custom_valdiation).empty:
                        raise Exception(
                            f"Custom validation failure for table: {table_name}!"
                        )
                    else:
                        print(f"Custom validation passed for table {table_name}!")


test = DataValidator(
    db_credentials={
        "host": "technical-test-1.cncti7m4kr9f.ap-south-1.rds.amazonaws.com",
        "port": 5432,
        "database": "technical_test",
        "username": "candidate",
        "password": "NW337AkNQH76veGc",
    },
    validation_config_path="validation.yml",
)
test.validate_custom_query()
