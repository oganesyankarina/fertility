import warnings
import pandas as pd
# import numpy as np

from datetime import date, datetime
# from dateutil import relativedelta as rdelta
# from operator import is_not
# from functools import partial
from create_connect_bd import cnx

warnings.filterwarnings('ignore')


def fertility_preprocessing():
    df = pd.read_sql_query('''select * from fertility''', cnx)
    df[['mother_year_of_birth', 'due_date_year',
        'due_date_month', 'mother_age']] = df[['mother_year_of_birth', 'due_date_year',
                                               'due_date_month', 'mother_age']].astype('int64')
    print(df.mother_address.sample(5))
    df = df[~df.mother_address.isin([''])]
    df.index = range(df.shape[0])
########################################################################################################################
# Первый прогон по адресам
    print('Первая обработка mother_address...')
    REGION = ['липецк ', 'елец ', 'воловский', 'грязинский', 'данковский', 'добринский', 'добровский', 'долгоруковский',
              'елецкий', 'задонский', 'измалковский', 'краснинский', 'лебедянский', 'лев-толстовский',
              'липецкий', 'становлянский', 'тербунский', 'усманский', 'хлевенский', 'чаплыгинский']
    start_time = datetime.now()
    for i in df.index[:]:
        for region in REGION:
            ind_str = df.loc[i, 'mother_address'].lower().find(region)
            df.loc[i, region] = ind_str
    print(f'Elapsed time {datetime.now() - start_time}')
    print(df[['mother_address', 'липецк ', 'елец ', 'воловский', 'грязинский', 'данковский', 'добринский', 'добровский',
              'долгоруковский', 'елецкий', 'задонский', 'измалковский', 'краснинский', 'лебедянский', 'лев-толстовский',
              'липецкий', 'становлянский', 'тербунский', 'усманский', 'хлевенский', 'чаплыгинский']][:5])
    print(len(df[['mother_address', 'липецк ', 'елец ', 'воловский', 'грязинский', 'данковский', 'добринский',
                  'добровский', 'долгоруковский', 'елецкий', 'задонский', 'измалковский', 'краснинский', 'лебедянский',
                  'лев-толстовский', 'липецкий', 'становлянский', 'тербунский', 'усманский', 'хлевенский',
                  'чаплыгинский']]))
    start_time = datetime.now()
    for i in df.index[:]:
        for col in REGION:
            if df.loc[i, col] >= 0:
                df.loc[i, 'Region'] = col
                break
    print(f'Elapsed time {datetime.now() - start_time}')
    print(df[['mother_address', 'Region']].sample(5))
########################################################################################################################
# Второй прогон по адресам
    print(f"Количество нераспознанных адресов: {len(df[df.Region.isna()][['mother_address', 'Region']][:])}")
    print('Вторая обработка mother_address...')
    CITIES_dict = {'волово': 'воловский', 'грязи': 'грязинский', 'данков': 'данковский',
                   'добринка': 'добринский', 'доброе': 'добровский', 'долгоруково': 'долгоруковский',
                   'задонск': 'задонский', 'измалково': 'измалковский', 'красное': 'краснинский',
                   'лебедянь': 'лебедянский', 'лев-толстой': 'лев-толстовский', 'становое': 'становлянский',
                   'тербуны': 'тербунский', 'усмань': 'усманский', 'хлевное': 'хлевенский', 'чаплыгин': 'чаплыгинский',
                   'лев толстой': 'лев-толстовский'}

    start_time = datetime.now()
    for i in df[df.Region.isna()].index[:]:
        for town in CITIES_dict.keys():
            ind_str = df.loc[i, 'mother_address'].lower().find(town)
            df.loc[i, CITIES_dict[town]] = ind_str
    print(f'Elapsed time {datetime.now() - start_time}')
    print(df[df.Region.isna()][['mother_address', 'Region',
                                'липецк ', 'елец ', 'воловский', 'грязинский', 'данковский', 'добринский', 'добровский',
                                'долгоруковский', 'елецкий', 'задонский', 'измалковский', 'краснинский', 'лебедянский',
                                'лев-толстовский', 'липецкий', 'становлянский', 'тербунский', 'усманский', 'хлевенский',
                                'чаплыгинский']][:5])
    print(len(df[df.Region.isna()][['mother_address', 'Region',
                                    'липецк ', 'елец ', 'воловский', 'грязинский', 'данковский', 'добринский',
                                    'добровский', 'долгоруковский', 'елецкий', 'задонский', 'измалковский',
                                    'краснинский', 'лебедянский', 'лев-толстовский', 'липецкий', 'становлянский',
                                    'тербунский', 'усманский', 'хлевенский', 'чаплыгинский']]))

    start_time = datetime.now()
    for i in df[df.Region.isna()].index[:]:
        for col in REGION:
            if df.loc[i, col] >= 0:
                df.loc[i, 'Region'] = col
                break
    print(f'Elapsed time {datetime.now() - start_time}')

    print(df[['mother_address', 'Region']].sample(5))
########################################################################################################################
# Удаляем строки с адресами за пределами Липецкой области
    print(f'Количество нераспознанных адресов: {len(df[df.Region.isna()])}')
    df = df[~df.Region.isna()]
    df.index = range(df.shape[0])
########################################################################################################################
# Финальная обработка названий районов
    print('Финальная обработка названий районов...')
    print(len(df.Region.unique()))
    print(sorted(df.Region.unique()))
    df.Region = df.Region.str.title()
    print(sorted(df.Region.unique()))

    return df

# table = pd.pivot_table(df, index=['due_date_year'], columns=['sex'], values=['which_born'],
# aggfunc='sum', margins=True)
# table


if __name__ == '__main__':
    fertility_preprocessing()
