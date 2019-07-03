import pandas as pd
import numpy as np
import gzip
import dask.dataframe as dd
import pandas as pd
import logging
import io
import argparse
from datetime import datetime

def read_data_file(filename):
    logging.info('Cargando dataframe')
    df = dd.read_csv(filename, delim_whitespace=True, header=None,
                     names=('fecha', 'hora', 'longitud', 'latitud',
                            'corriente_pico', 'multiplicidad', 'idk1',
                            'idk2', 'idk3'), compression='gzip',
                     blocksize=None)
    logging.info('Dataframe cargado')
    data_time = df['fecha'] + ' ' + df['hora']

    df = df.drop('fecha', axis=1).drop('hora', axis=1).drop('idk1', axis=1)
    df = df.drop('idk2', axis=1).drop('idk3', axis=1)
    df['datetime'] = data_time.map(
            lambda s: datetime.strptime(str(s), '%m/%d/%y %H:%M:%S'),
            meta=pd.Series([], dtype='datetime64[ns]'))

    logging.info('Creando indice de fecha')
    df = df.set_index('datetime')
    logging.info('Indice creado')

    return df

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Cargando dataframe')
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('filename', metavar='Filename', type=str, nargs=1,
                        help='dirección del archivo con los datos de rayos')
    parser.add_argument('--output', nargs='?', help='no u')
    args = parser.parse_args()


    df = read_data_file(args.filename)

    output_filename = (args.output if args.output is not None 
                       else '{}-parquet'.format(args.filename[0]))
    logging.info('Escribiendo archivos de parquet')
    df.to_parquet(output_filename, compression='snappy')

