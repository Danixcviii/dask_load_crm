import time
from email_validator import validate_email as email_check, EmailNotValidError
import re 
start_time = time.perf_counter()
import dask.delayed
from dask.distributed import Client
import dask.dataframe as dd
import dask
from pandera import Column, Check, DataFrameSchema
import pandera as pa
import pandas as pd
from datetime import datetime
import sys
import json


@dask.delayed
def read_excel_dask(path: str):
    df = pd.read_excel(path, dtype=str)
    return dd.from_pandas(df, chunksize=__CHUNKSIZE__)

@dask.delayed
def read_metafile(path: str) -> dict:
    with open(path) as f:
        return json.loads(f.read())
    
@dask.delayed
def get_sections():
    with open('/home/data/sections.txt') as f:
        return f.read()

def datetime_validator(data: str, format:str=r'%Y-%m-%d') -> bool:
    try:
        datetime.strptime(data, format)
        return True
    except: return False

def validate_email(email: str) -> bool:
    try:
        email_check(email)  # Validate the email format
        return True
    except: return False

def generate_panderas_column(rules:str) -> Column:
    required, *other_rules = rules.split("|")
    if other_rules[0].startswith("digits_between"):
        numbers = other_rules[0].split(":")[1]
        min_length, max_length = map(int, numbers.split(","))
        checks = [Check.str_length(min_length, max_length)]
    elif other_rules[0] == "date":
        checks = [Check(lambda s: s.apply(datetime_validator))]
    elif other_rules[0] == "string":
        min_length = int(other_rules[1].split(":")[1])
        max_length = int(other_rules[2].split(":")[1])
        checks = [Check.str_length(min_length, max_length)]
    elif other_rules[0] == "time":
        checks = [Check(lambda s: s.apply(datetime_validator, args=(r'%H:%M:%S', )))]
    elif other_rules[0] == "email":
        min_length = int(other_rules[1].split(":")[1])
        max_length = int(other_rules[2].split(":")[1])
        checks = [Check(lambda s: s.apply(validate_email))]
        # checks = [Check.str_length(min_length, max_length)]

    return Column(pa.String, checks, required=required, nullable=(not required))


def get_valids(chunk: pd.DataFrame, schema: DataFrameSchema):
    try:
        valid_rows = schema(chunk, lazy=True)
        return valid_rows
    except pa.errors.SchemaErrors as e:
        # invalid_rows = chunk.loc[uniq_index:=]
        valid_rows = chunk.drop(index=e.failure_cases['index'].drop_duplicates())
        return valid_rows
    
def get_form_answers(chunk: pd.DataFrame, sections: str, report_fields: list, typification_fields: list, client_fields: list):
    forms_answers = pd.DataFrame()
    forms_answers['index_data'] = chunk.to_dict('records')
    forms_answers['structure_answer'] = sections

    for index, row in forms_answers.iterrows():
        values = row['index_data']
        for key, value in values.items():
            forms_answers.at[index, 'structure_answer'] = forms_answers.loc[index, 'structure_answer'].replace(f'@@{key}@@', value)

    forms_answers['values_for_reports'] = chunk[report_fields].to_dict('records') if report_fields else '{}'
    forms_answers['values_for_typification'] = chunk[typification_fields].to_dict('records') if typification_fields else '{}'
    forms_answers['client_fields'] = chunk[client_fields].to_dict('records') if client_fields else '{}'
    return forms_answers


if __name__ == '__main__':
    __CHUNKSIZE__ = 100000
    __UNIQUENAME__ = sys.argv[1]

    Client('172.10.7.224:8786')

    metafile = read_metafile(f'/home/data/{__UNIQUENAME__}.json').compute()
    ddf = read_excel_dask(f'/home/data/{__UNIQUENAME__}.xlsx').drop_duplicates()

    ids = {key: str(item['id']) for key, item in metafile['valdict'].items()}
    client_unique, *_ = [str(item['id']) for item in metafile['valdict'].values() if item['structure_to_save']['client_unique']]
    validation_rules = {str(item['id']): item['validation_rule'] for item in metafile['valdict'].values()}

    ddf = ddf.rename(columns=ids)

    schema = DataFrameSchema(
        {key: generate_panderas_column(item) for key, item in validation_rules.items()}
    )

    valid_ddf = ddf.map_partitions(get_valids, schema, meta=ddf)

    sections = get_sections().compute()

    report_fields = [str(field['id']) for field in metafile['valdict'].values() if field['structure_to_save']['inReport']]
    typification_fields = [str(field['id']) for field in metafile['valdict'].values() if field['structure_to_save']['isTypificated']]
    client_fields = [str(field['id']) for field in metafile['valdict'].values() if field['structure_to_save']['isClientInfo']]

    form_answers = valid_ddf.map_partitions(get_form_answers,
                                            sections,
                                            report_fields,
                                            typification_fields,
                                            client_fields,
                                            meta={
                                                    'index_data': str,
                                                    'structure_answer': str,
                                                    'values_for_reports': str,
                                                    'values_for_typification': str,
                                                    'client_fields': str
                                                })

    form_answers.to_sql('tb_forms_answers', 'mysql+pymysql://josesuarez4005:MmqLF,wdZofZyJjlsafQ@172.10.7.224:3306/dba_load_testing',
                        parallel=True,
                        index=False,
                        if_exists='replace').compute()

    print(f"exectime: {time.perf_counter() - start_time: .2f} s")