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

def datetime_validator(data: str, format:str) -> bool:
    try:
        datetime.strptime(data, format)
        return True
    except: return False

def validate_email(email: str, min_length: int, max_length: int) -> bool:
    try:
        email_check(email)  # Validate the email format
        if min_length <= len(email) <= max_length:
            return True
    except EmailNotValidError:
        return False
    return False

def generate_panderas_column(rules:str) -> Column:
    rulesList = rules.split("|")
    required = rulesList[0]  
    other_rules = rulesList[1:]
  
    if required == "required":
        if other_rules[0].startswith("digits_between:"):
            numbers = other_rules.split(":")[1]
            min_length, max_length = map(int, numbers.split(","))

            return Column(pa.String, Check.str_length(min_length, max_length), required=True)
        
        elif other_rules[0] == "date":
            return Column(str, Check(lambda s: s.apply(datetime_validator, args=(r'%Y-%m-%d'))), required=True)
        
        elif other_rules[0] == "string":
            min_length = int(other_rules[1].split(":")[1])
            max_length = int(other_rules[2].split(":")[1])

            return Column(pa.String, Check.str_length(min_length, max_length), required=True)

        elif other_rules[0] == "time":
            return Column(str, Check(lambda s: s.apply(datetime_validator, args=(r'%H:%M:%S'))), required=True)
        
        elif other_rules[0] == "email":
       
            min_length = int(other_rules[1].split(":")[1])
            max_length = int(other_rules[2].split(":")[1])
    
            return Column(str, Check(lambda s: s.apply(validate_email, args=(min_length, max_length))), required=True)


    else:
        if other_rules[0].startswith("digits_between:"):
            numbers = other_rules.split(":")[1]
            min_length, max_length = map(int, numbers.split(","))
            return Column(pa.String, Check.str_length(min_length, max_length), required=False)
        
        elif other_rules[0] == "date":
            return Column(str, Check(lambda s: s.apply(datetime_validator, args=('%Y-%m-%d'))), required=False)
        
        elif other_rules[0] == "string":
            min_length = int(other_rules[1].split(":")[1])
            max_length = int(other_rules[2].split(":")[1])
            return Column(pa.String, Check.str_length(min_length, max_length), required=False)
        
        elif other_rules[0] == "time":
            return Column(str, Check(lambda s: s.apply(datetime_validator, args=(r'%H:%M:%S'))), required=False)
        
        elif other_rules[0] == "email":
            min_length = int(other_rules[1].split(":")[1])
            max_length = int(other_rules[2].split(":")[1])

            return Column(str, Check(lambda s: s.apply(validate_email, args=(min_length, max_length))), required=False)


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



__CHUNKSIZE__ = 100000
__UNIQUENAME__ = sys.argv[1]

Client('172.10.7.224:8786')

metafile = read_metafile(f'/home/data/{__UNIQUENAME__}.json').compute()
ddf = read_excel_dask(f'/home/data/{__UNIQUENAME__}.xlsx').drop_duplicates()

ids = {key: str(item['id']) for key, item in metafile['valdict'].items()}
client_unique, *_ = [item['id'] for item in metafile['valdict'].values() if item['structure_to_save']['client_unique']]

ddf = ddf.rename(columns=ids)

schema = DataFrameSchema(
    {
        "1727469668797": Column(pa.String, Check.str_length(10, 12), required=True), # required|digits_between:10,12
        "1727469904582": Column(str, Check(lambda s: s.apply(datetime_validator, args=(r'%Y-%m-%d')))), # required|date|date_format:Y-m-d
        "1727470064986": Column(str, Check.str_length(8, 9), required=True), # required|string|min:8|max:9
        "1727470179642": Column(str, Check.str_length(12, 24), nullable=True), # nullable|string|min:12|max:24
        "1727470303727": Column(str, Check.str_length(1, 50), required=True), # required|string|min:1|max:19
        "1727470400400": Column(str, Check.str_length(1, 50), required=True), # required|string|min:1|max:50
        "1727470439915": Column(str, Check.str_length(1, 20), required=True), # required|string|min:1|max:20
        "1727470519236": Column(str, Check.str_length(10, 12), required=True), # required|string|min:10|max:12
        "1727470586070": Column(str, Check.str_length(10, 100), required=True) # required|email|min:10|max:100
    }
)

valid_ddf = ddf.map_partitions(get_valids, schema, meta=ddf).drop_duplicates()

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

print(f'form answers build: {time.perf_counter() - start_time: .2f} s')

form_answers.to_sql('tb_forms_answers', 'mysql+pymysql://josesuarez4005:MmqLF,wdZofZyJjlsafQ@172.10.7.224:3306/dba_load_testing',
                    parallel=True,
                    index=False,
                    if_exists='replace').compute()

print(f"exectime: {time.perf_counter() - start_time: .2f} s")