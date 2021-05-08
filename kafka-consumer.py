import json
from time import sleep
from google.api_core.exceptions import BadRequest
from kafka import KafkaConsumer
from google.cloud import bigquery

client = bigquery.Client()
ds = 'transaction'
DATASET = '{}.{}'.format(client.project, ds)


def query_builder(columns, values):
    wheres = []
    for i in range(len(columns)):
        if isinstance(values[i], str):
            wheres.append(f'{columns[i]} = "{values[i]}"')
        else:
            wheres.append(f'{columns[i]} = {values[i]}')
    return " and ".join(wheres)


def column_builder(columns):
    column = '('
    column += ', '.join(map(str, columns))
    column += ')'

    return column


def value_builder(values):
    value = '('
    for val in values:
        if isinstance(val, str):
            value += f'"{val}"'
        else:
            value += str(val)

        if val != values[-1]:
            value += ', '

    value += ')'

    return value


consumer = KafkaConsumer('api-gatekeeper')
for msg in consumer:
    # Deserialize Json
    Payload = json.loads(msg.value)

    # Looping through activities
    for activity in Payload['activities']:
        tables = client.list_tables(DATASET)
        bq_table_name = [table.table_id for table in tables]

        if activity['operation'] == 'insert':
            if activity['table'] not in bq_table_name:
                schema = []

                for i in range(len(activity['col_names'])):
                    schema.append(
                        bigquery.SchemaField(
                            activity['col_names'][i], activity['col_types'][i])
                    )

                table = bigquery.Table(
                    DATASET+f'.{activity["table"]}', schema=schema)
                table = client.create_table(table)

                print(
                    "Created table {}.{}.{}".format(
                        table.project, table.dataset_id, table.table_id)
                )

                rows_to_insert = [
                    {activity['col_names'][i]: activity['col_values'][i]
                        for i in range(len(activity['col_names']))},
                ]

                errors = client.insert_rows_json(
                    "{}.{}.{}".format(
                        table.project, table.dataset_id, table.table_id),
                    rows_to_insert,
                    row_ids=[None] * len(rows_to_insert)
                )

                if errors == []:
                    print("New rows have been added.")
                else:
                    print("Encountered errors while inserting rows: {}".format(errors))

            if activity['table'] in bq_table_name:
                table = client.get_table(
                    DATASET+f'.{activity["table"]}')

                original_schema = table.schema
                original_schema_name = [x.name for x in original_schema]

                num_of_field_original = len(original_schema_name)

                for i in range(len(activity['col_names'])):
                    if activity['col_names'][i] not in original_schema_name:
                        original_schema.append(
                            bigquery.SchemaField(activity['col_names'][i], activity['col_types'][i]))

                table.schema = original_schema

                try:
                    table = client.update_table(table, ["schema"])
                except BadRequest:
                    pass

                query = f"""
                INSERT {ds}.{activity['table']}{column_builder(activity['col_names'])}
                VALUES{value_builder(activity['col_values'])}
                """

                query_job = client.query(query)  # API request
                query_job.result()  # Waits for statement to finish

                print(
                    f"Row with {query_builder(activity['col_names'], activity['col_values'])} added.")

        elif activity['operation'] == 'delete':
            if activity['table'] not in bq_table_name:
                print("Transaction Failed, table {} not found".format(
                    activity['table']))
            elif activity['table'] in bq_table_name:
                query = f"""
                DELETE FROM {DATASET}.{activity['table']}
                WHERE {query_builder(activity['col_names'], activity['col_values'])}
                """

                query_job = client.query(query)  # API request
                query_job.result()  # Waits for statement to finish
                print(
                    f"Row with {query_builder(activity['col_names'], activity['col_values'])} deleted.")
