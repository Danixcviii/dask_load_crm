from dask.distributed import Client
client = Client('172.10.7.224:8786')
client.upload_file('./src/utils.py')
client.upload_file('./src/pandera_parser.py')

import sys
from utils import read_excel_dask, read_metafile, get_valids, get_invalids, get_clients
from pandera_parser import PanderaParser
import pandera as pa
import pandas as pd

if __name__ == '__main__':
    __CHUNKSIZE__ = 100000
    __FILENAME__ = sys.argv[1]

    metafile = read_metafile(f'/home/data/{__FILENAME__}.json').compute()
    ddf = read_excel_dask(f'/home/data/{__FILENAME__}.xlsx', __CHUNKSIZE__).drop_duplicates()

    ids = {column_name: str(item['id']) for column_name, item in metafile['valdict'].items()}

    ddf = ddf.rename(columns=ids).persist()

    validation_rules = {str(item['id']): item['validation_rule'] for item in metafile['valdict'].values()}
    schema = PanderaParser().parse_schema(validation_rules)
    
    valid_ddf = ddf.map_partitions(get_valids, schema, meta=ddf).persist()

    invalid_ddf = ddf.map_partitions(get_invalids,
                                     schema,
                                     meta={
                                         'schema_context': str,
                                         'column': str,
                                         'check': str,
                                         'check_number': str,
                                         'failure_case': str,
                                         'index': str,
                                     })
    
    (client_unique, structure_to_save), *_ = [(str(item['id']), item['structure_to_save']) 
                            for item in metafile['valdict'].values() if item['structure_to_save']['client_unique']]
    
    clients = get_clients("mysql+pymysql://aplic_bigdata:6n1gz4f2UIxS@172.17.9.31:3306/miosv2_crm2_qa",
                          metafile['form_id'],
                          client_unique)
    
    # new_clients = clients.join(valid_ddf.set_index(client_unique))
    new_clients = valid_ddf.set_index(client_unique).join(clients, how='right')

    print(invalid_ddf[['check', 'failure_case', 'index']].head().compute())
    print(valid_ddf.head().compute())
    print(clients.head().compute())
    print(new_clients.head().compute())
