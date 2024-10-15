import dask.dataframe as dd
import dask
import dask.delayed
import pandera as pa
import pandas as pd
import json

def get_valids(chunk: pd.DataFrame, schema: pa.DataFrameSchema):
    try:
        return schema(chunk, lazy=True)
    except pa.errors.SchemaErrors as e:
        return chunk.drop(index=e.failure_cases['index'].drop_duplicates())

def get_invalids(chunk: pd.DataFrame, schema: pa.DataFrameSchema):
    try:
        schema(chunk, lazy=True)
        return pd.DataFrame()
    except pa.errors.SchemaErrors as e:
        return e.failure_cases

@dask.delayed
def read_excel_dask(path: str, chunksize:int):
    df = pd.read_excel(path, dtype=str)
    return dd.from_pandas(df, chunksize=chunksize)

@dask.delayed
def read_metafile(path: str) -> dict:
    with open(path) as f:
        return json.loads(f.read())
    
@dask.delayed
def get_clients(uri: str, form_id: str, client_unique: str) -> pd.DataFrame:
    return pd.read_sql(f"""
            SELECT 
                id,
                JSON_EXTRACT(unique_indentificator, '$[0].id') `field_id`,
                JSON_EXTRACT(unique_indentificator, '$[0].value') `uniquecode`
            FROM
                miosv2_crm2_qa.client_news
            WHERE
                form_id = {form_id}
            HAVING
                `field_id` = {client_unique}; 
                """, uri, index_col='uniquecode')