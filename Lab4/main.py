import psycopg2
import pandas as pd
import os

from psycopg2 import sql


def generate_create_table_script(file_path):
    table_name = os.path.splitext(os.path.basename(file_path))[0]

    sample_data = pd.read_csv(file_path, nrows=5)

    column_definitions = []
    for column_name, data_type in zip(sample_data.columns, sample_data.dtypes):
        if data_type == 'int64':
            column_type = 'INT'
        elif data_type == 'float64':
            column_type = 'FLOAT'
        else:
            column_type = 'VARCHAR(255)'
        column_definitions.append(f'{column_name} {column_type}')

    create_table_script = f"""
CREATE TABLE {table_name} (
    {', '.join(column_definitions)}
);
    """
    return create_table_script


def generate_insert_script(file_path, table_name):
    df = pd.read_csv(file_path)

    insert_script = sql.SQL("""
INSERT INTO {table_name} ({columns})
VALUES
""").format(
        table_name=sql.Identifier(table_name),
        columns=sql.SQL(', ').join(map(sql.Identifier, df.columns))
    )

    values = []
    for _, row in df.iterrows():
        quoted_row = [f"'{value}'" if pd.notna(value) and isinstance(value, str) else value for value in row]
        values.append(sql.SQL("({})").format(sql.SQL(', ').join(map(sql.Literal, quoted_row))))

    insert_script += sql.SQL(',\n').join(values) + sql.SQL(";\n")

    return insert_script

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cur = conn.cursor()

    csv_directory = "data"

    for file_name in os.listdir(csv_directory):
        if file_name.endswith('.csv'):
            file_path = os.path.join(csv_directory, file_name)
            table_name = os.path.splitext(file_name)[0]

            create = generate_create_table_script(file_path)
            print(create)
            cur.execute(create)

            insert = generate_insert_script(file_path, table_name).as_string(cur)
            print(insert)


if __name__ == "__main__":
    main()
