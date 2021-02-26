import warnings
import pandas as pd

from datetime import datetime
from create_connect_bd import cnx

warnings.filterwarnings('ignore')


def fertility_preprocessing(save_to_sql=False, save_to_excel=False):
    df = pd.read_sql_query('''select * from fertility''', cnx)
    df[['mother_year_of_birth', 'due_date_year',
        'due_date_month', 'mother_age']] = df[['mother_year_of_birth', 'due_date_year',
                                               'due_date_month', 'mother_age']].astype('int64')
    print(f'Количество записей: {len(df)}')
    df = df[~df.mother_address.isin([''])]
    df.index = range(df.shape[0])
    print(f'Количество записей после удаления строк без адресов: {len(df)}')
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
    for i in df.index[:]:
        for col in REGION:
            if df.loc[i, col] >= 0:
                df.loc[i, 'Region'] = col
                break
    print(f'Elapsed time {datetime.now() - start_time}')
    print(f'Количество записей: {len(df)}')
    print(df[['mother_address', 'Region']].sample(3))
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
    for i in df[df.Region.isna()].index[:]:
        for col in REGION:
            if df.loc[i, col] >= 0:
                df.loc[i, 'Region'] = col
                break
    print(f'Elapsed time {datetime.now() - start_time}')
    print(f'Количество записей: {len(df)}')
    print(df[['mother_address', 'Region']].sample(3))
########################################################################################################################
# Удаляем строки с адресами за пределами Липецкой области
    print(f"Количество нераспознанных адресов: {len(df[df.Region.isna()][['mother_address', 'Region']][:])}")
    df = df[~df.Region.isna()]
    df.index = range(df.shape[0])
########################################################################################################################
# Финальная обработка названий районов
    print('Финальная обработка названий районов...')
    print(f'Количество районов: {len(df.Region.unique())}')
    print(sorted(df.Region.unique()))
    df.Region = df.Region.str.title()
    print(sorted(df.Region.unique()))
    df = df.drop(columns=REGION)
    print(f'Количество записей: {len(df)}')
########################################################################################################################
# Сохраняем предобработанные данные
    if save_to_sql:
        print('Сохраняем данные в базу данных...')
        # df_death.to_sql('death_finished', cnx, if_exists='replace', index_label='id')
    if save_to_excel:
        print('Сохраняем данные в файл...')
        path = r'C:\Users\oganesyanKZ\PycharmProjects\fertility\Рассчеты/'
        with pd.ExcelWriter(f'{path}fertility_preprocessed.xlsx', engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=f'fertility_preprocessed', header=True, index=False, encoding='1251')
########################################################################################################################
    return df
########################################################################################################################


if __name__ == '__main__':
    fertility_preprocessing(save_to_excel=True)
