#!/Users/gene/.virtualenvs/futures/bin/python

import argparse as ap
import psycopg2 as pg
import os
from subprocess import call
import os.path as op
import requests as rq


def get_api_key():
    parser = ap.ArgumentParser()
    parser.add_argument('apikey', help='Please provide a api key')
    args = parser.parse_args()
    return args.apikey


def get_params(apikey, symbol_str):
    params = {'apikey': apikey, 'symbol': symbol_str,
              'type': 'daily', 'startDate': '19500101'}
    return params


def get_symbol_str(symbol, month_code, year_code):
    symbol_str = symbol + month_code + str(year_code).zfill(2)
    return symbol_str


def get_filename(symbol_str, ext='.csv'):
    filename = symbol_str + ext
    return filename


def get_cursor():
    conn = pg.connect(os.environ.get('DATABASE_URL'))
    cur = conn.cursor()
    return cur


def get_file_path(filename, symbol):
    download_folder_name = 'downloads'
    download_folder_path = op.join(op.dirname(__file__),
                                   download_folder_name,
                                   symbol)
    if not op.exists(download_folder_path):
        call(['mkdir', '-p', download_folder_path])
    file_path = op.join('.', download_folder_path, filename)
    return file_path



def download(base_url, params, file_path):
    with rq.get(base_url, params=params, stream=True) as r:
        if len(r.text) < 100:
            print '{} does not exist!'.format(file_path)
            pass
        else:
            with open(file_path, 'wb') as f:
                f.write(r.text)
                print 'Downloading {} finished!'.format(file_path)


def run_downloader():
    base_url = 'http://ondemand.websol.barchart.com/getHistory.csv'
    download_folder_name = 'downloads'
    years = set(range(100)) - set(range(25, 95))
    months = list('FGHJKMNQUVXZ')
    apikey = get_api_key()
    cur = get_cursor()
    cur.execute('SELECT symbol FROM symbols')
    queries = cur.fetchall()
    for query in queries:
        symbol = query[0]
        download_folder_path = op.join(op.dirname(__file__),
                                       download_folder_name,
                                       symbol)
        if len(rq.get(base_url,
                  params = {'apikey': apikey, 'symbol': symbol + '*1',
                            'type': 'daily', 'startDate': '19500101'}).text) < 10:
            print 'This {} is Not included in the subscription'.format(symbol)
        else:
            if op.exists(download_folder_path):
                print "{} already existed!".format(symbol)
                pass
            else:
                for year_code in years:
                    for month_code in months:
                        symbol_str = get_symbol_str(symbol=symbol,
                                                    month_code=month_code,
                                                    year_code=year_code)
                        params = get_params(apikey=apikey, symbol_str=symbol_str)
                        filename = get_filename(symbol_str=symbol_str)
                        file_path = get_file_path(filename=filename, symbol=symbol)
                        download(base_url=base_url, params=params, file_path=file_path)


run_downloader()
