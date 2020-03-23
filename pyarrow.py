import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

connection = pg.connect("host='aws.rds.amazonaws.com' dbname=postgres user=your_user password='your_pass'")

#dataframe = psql.DataFrame("SELECT * FROM category", connection)

df = pd.read_sql_query('select * from your_table',con=connection)

print(df)

# Coloca na tabela pyarrow

table = pa.Table.from_pandas(df)

# Gera o arquivo parquet

pq.write_table(table, 'exemplo.parquet')

# Ler o arquivo parquet

table2 = pq.read_table('exemplo.parquet')

# Imprime no formato parquet

table2.to_pandas()
