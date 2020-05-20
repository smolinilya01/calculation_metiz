"""Search nomenclature analog
Поиск идентичной или аналогичной номенклатуры.
Алогритм как write_off"""

from algo.write_off import write_off
from etl.extract import (
    replacements, load_orders_to_supplier, nomenclature
)
from etl.extract import (
    PATH_REP_MARK, PATH_REP_GOST,
    PATH_REP_POKR, PATH_REP_PROCHN
)
from common.common import check_calculation_right
from datetime import datetime
from reports.weekly import weekly_tables
from reports.excel import weekly_excel_reports
from pandas import (DataFrame, concat, read_csv, read_excel)
from etl.extract import NOW


def building_purchase_analysis() -> DataFrame:
    """Повторение алгоритма расчета потребности на файле из прошлого"""
    sep_date = separate_date()

    operations = list()
    dict_nom = nomenclature()
    dict_repl_mark = replacements(PATH_REP_MARK)
    dict_repl_gost = replacements(PATH_REP_GOST)
    dict_repl_pokr = replacements(PATH_REP_POKR)
    dict_repl_prochn = replacements(PATH_REP_PROCHN)

    start_rest_center = void_rests(dict_nom=dict_nom)
    start_rest_tn = void_rests(dict_nom=dict_nom)

    start_fut = modify_orders_to_supplier(table=load_orders_to_supplier(), dict_nom=dict_nom)
    start_ask = old_requirements()

    end_rest_center = start_rest_center.copy()
    end_rest_tn = start_rest_tn.copy()
    end_fut = start_fut.copy()
    end_ask = start_ask.copy()

    # списание остатков на потребности
    end_ask, end_rest_tn, end_rest_center, end_fut, operations = write_off(
        table=end_ask,
        rest_tn=end_rest_tn,
        rest_c=end_rest_center,
        fut=end_fut,
        oper_=operations,
        nom_=dict_nom,
        repl_={
            'mark': dict_repl_mark,
            'gost': dict_repl_gost,
            'pokr': dict_repl_pokr,
            'prochn': dict_repl_prochn
        }
    )

    check_calculation_right(
        start_ask_=start_ask,
        end_ask_=end_ask,
        start_c_=start_rest_center,
        end_c_=end_rest_center,
        start_tn_=start_rest_tn,
        end_tn_=end_rest_tn,
        start_fut_=start_fut,
        end_fut_=end_fut,
    )

    weekly_tables(
        start_ask_=start_ask,
        end_ask_=end_ask,
        oper_=operations,
        sep_date=sep_date
    )
    weekly_excel_reports()

    # построение таблицы-----------------------------------------------
    name_oper = r'.\support_data\output_tables\oper_{0}.csv'.format(NOW.strftime('%Y%m%d'))
    data = read_csv(
        name_oper,
        sep=";",
        encoding='ansi',
        usecols=[0, 3, 7, 10],
        parse_dates=['Дата потребности']
    )
    data = data[data['Дата потребности'] <= sep_date]\
        [['Номенклатура потребности', 'Номенклатура Списания', 'Списание потребности']]
    data = data.\
        groupby(by=['Номенклатура потребности', 'Номенклатура Списания']).\
        sum().\
        reset_index().\
        rename(columns={'Номенклатура потребности': 'Номенклатура',
                        'Номенклатура Списания': 'Номенклатура_заказа',
                        "Списание потребности": 'План_закупа'})
    data['Заказано'] = data['План_закупа']
    data = data[['Номенклатура', 'План_закупа', 'Номенклатура_заказа', 'Заказано']]

    additional_plan = end_ask.copy()  # план закупа, который остался без заказов поставщикам
    additional_plan = additional_plan[additional_plan['Дата запуска'] <= sep_date]\
        [['Номенклатура', 'Дефицит']].\
        groupby(by=['Номенклатура']).\
        sum().\
        reset_index().\
        rename(columns={'Дефицит': 'План_закупа'})
    additional_plan['Номенклатура_заказа'] = None
    additional_plan['Заказано'] = 0
    additional_plan = additional_plan[['Номенклатура', 'План_закупа', 'Номенклатура_заказа', 'Заказано']]
    additional_plan = additional_plan[additional_plan['План_закупа'] > 0]

    data = concat((data, additional_plan))

    additional_futures = end_fut.copy()
    additional_futures = additional_futures[additional_futures['Количество'] > 0].\
        groupby(by=['Номенклатура'])\
        ['Количество'].\
        sum().\
        reset_index().\
        rename(columns={'Номенклатура': 'Номенклатура_заказа',
                        'Количество': 'Заказано'})
    additional_futures['Номенклатура'] = None
    additional_futures['План_закупа'] = 0
    additional_futures = additional_futures[
        ['Номенклатура', 'План_закупа', 'Номенклатура_заказа', 'Заказано']
    ]

    data = concat((data, additional_futures))

    # добавление Поступило и остаточная потребность-------------------------------------
    inputs = load_orders_to_supplier()
    inputs = inputs[['Номенклатура', 'Заказано', 'Доставлено']].\
        rename(columns={'Номенклатура': 'Номенклатура_заказа',
                        'Заказано': 'Заказано_всего'}).\
        fillna(0)
    data = data.\
        merge(inputs, on='Номенклатура_заказа', how='left').\
        fillna(0)
    data['Процент_заказа'] = data['Заказано'] / data['Заказано_всего']
    data['Доставлено'] = data['Доставлено'] * data['Процент_заказа']

    data['Еще_заказать'] = data['План_закупа'] - data['Заказано']
    data['Еще_заказать'] = data['Еще_заказать']. \
        where(data['Еще_заказать'] > 0, 0)
    data = data. \
        sort_values(by=['Номенклатура', 'Номенклатура_заказа'])

    data = data[[
        'Номенклатура', 'План_закупа', 'Номенклатура_заказа',
        'Заказано', 'Доставлено', 'Еще_заказать'
    ]].fillna(0)

    data.\
        rename(columns={'Дефицит': 'План_закупа', 'Еще_заказать': 'Остаточная_потребность'}).\
        to_excel(
            r".\support_data\purchase_analysis\purchase_analysis.xlsx",
            index=False
        )

    return data


def separate_date() -> datetime:
    """Возвращает дату краткосрочного закупа"""
    SEPARATE_DATE_PATH = r".\support_data\purchase_analysis\Итоговая_потребность.xlsm"
    data = read_excel(
        SEPARATE_DATE_PATH,
        sheet_name='Списания',
        header=0,
        usecols=[0],
        parse_dates=['Дата запуска']
    )
    sep_date = data['Дата запуска'].max()

    return sep_date


def old_requirements() -> DataFrame:
    """Подгружает потребности из файла ask.csv"""
    data = read_csv(
        r".\support_data\purchase_analysis\ask.csv",
        sep=";",
        encoding='ansi',
        parse_dates=['Дата запуска']
    )
    data['Количество в заказе'] = data['Остаток дефицита']
    data['Перемещено'] = 0
    data['Дефицит'] = data['Остаток дефицита']

    del data['Остаток дефицита'], data['Списание из Цент склада'], \
        data['Списание из ТН'], data['Списание из Поступлений']

    data = data[(data['Дефицит'] > 0) & (data['Дата запуска'] <= separate_date())]

    return data


def void_rests(dict_nom: DataFrame) -> DataFrame:
    """Возвращает пустые остатки, нужно, что бы основной алгоритм работал

    :param dict_nom: справочник номенклатур
    """
    columns = ["Дата", "Номенклатура", "Количество", "Склад"]
    data = DataFrame(data=None, columns=columns)
    data = data.merge(dict_nom, on='Номенклатура', how='left')

    return data


def modify_orders_to_supplier(table: DataFrame, dict_nom: DataFrame) -> DataFrame:
    """Модифицирует закупки в список futures

    :param table: таблица из load_orders_to_supplier()
    :param dict_nom: справочник номенклатур
    """
    data = table.copy()
    data = data[['Номенклатура', 'Заказано']].\
        rename(columns={'Заказано': "Количество"})
    data['Дата'] = datetime.now()
    data['Склад'] = 'Поступления'

    data = data.\
        fillna(0).\
        sort_values(by='Дата')
    data = data.merge(dict_nom, on='Номенклатура', how='left')

    return data
