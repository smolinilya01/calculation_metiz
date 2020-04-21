"""Extract data"""

import logging

from common.common import (
    modify_col, replace_minus, extract_product_name,
    in_float, multiple_sort
)
from pandas import (
    DataFrame, read_csv, merge,
    read_sql_query, concat
)
from datetime import datetime, timedelta
from pypyodbc import connect


NOW: datetime = datetime.now()
DAYS_AFTER: int = 4  # для расчета дневного дефицита, определеяет период от сегодня + 4 дня
PATH_REP_MARK = r'.\support_data\outloads\dict_replacement_marka.csv'
PATH_REP_GOST = r'.\support_data\outloads\dict_replacement_gost.csv'
PATH_REP_POKR = r'.\support_data\outloads\dict_replacement_pokrit.csv'
PATH_REP_PROCHN = r'.\support_data\outloads\dict_replacement_prochn.csv'
PATH_LON_SORT = r'support_data/outloads/long_term_sortaments.csv'


def requirements(short_term_plan: bool = False) -> DataFrame:
    """Загузка таблицы с первичной потребностью (дефицитом), форматирование таблицы."""
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Расчет метизы (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
        parse_dates=['Дата запуска', 'Дата начала факт', 'Дата поступления КМД'],
        dayfirst=True
    )
    if short_term_plan is True:  # для краткосрочного планирования сразу обрезаем по дате
        end_date = NOW + timedelta(days=DAYS_AFTER)
        data = data[data['Дата запуска'] <= end_date]

    data = data[~data['Номенклатура'].isna()]
    data = data.fillna(value=0)
    data = data.rename(columns={'Обеспечена метизы': 'Заказ обеспечен'})
    data['Заказ обеспечен'] = data['Заказ обеспечен'].replace({'Нет': 0, 'Да': 1})
    data['Пометка удаления'] = data['Пометка удаления'].replace({'Нет': 0, 'Да': 1})
    data['Номер победы'] = modify_col(data['Номер победы'], instr=1, space=1)
    data['Партия'] = data['Партия'].map(int)
    data['Партия'] = modify_col(data['Партия'], instr=1, space=1).replace({'0': '1', '0.0': '1'})
    data['Количество в заказе'] = modify_col(data['Количество в заказе'], instr=1, space=1, comma=1, numeric=1)
    data['Дефицит'] = modify_col(data['Дефицит'], instr=1, space=1, comma=1, numeric=1).map(replace_minus)
    data['Перемещено'] = modify_col(data['Перемещено'], instr=1, space=1, comma=1, numeric=1, minus=1)
    data['Заказ обеспечен'] = modify_col(data['Заказ обеспечен'], instr=1, space=1, comma=1, numeric=1)
    data['Пометка удаления'] = modify_col(data['Пометка удаления'], instr=1, space=1, comma=1, numeric=1)
    data['Заказ-Партия'] = data['Номер победы'] + "-" + data['Партия']
    data['Изделие'] = modify_col(data['Изделие'], instr=1).map(extract_product_name)
    data['Нельзя_заменять'] = 0  # в будущем в выгрузку добавиться колонка о запрете замены
    data['Количество штук'] = data['Количество штук'].map(in_float)
    data['Документ заказа.Статус'] = data['Документ заказа.Статус'].map(in_float)

    # добавляет колонки 'Закуп подтвержден', 'Возможный заказ' по данным из ПОБЕДЫ
    appr_orders = approved_orders(tuple(data['Номер победы'].unique()))
    data = merge(data, appr_orders, how='left', on='Номер победы', copy=False)

    # расчет Дефицита с фильтрами
    non_complect_orders = non_complect()
    order_shipments = order_shipment()
    data = data.\
        merge(non_complect_orders, how='left', on=['Номер победы', 'Номенклатура']).\
        merge(order_shipments, how='left', on='Номер победы')
    data['Некомплектная_отгрузка'] = data['Некомплектная_отгрузка'].fillna(0)
    data['Полная_отгрузка'] = data['Полная_отгрузка'].fillna(0)
    data['Дефицит'] = (data['Количество в заказе'] - data['Перемещено']).\
        map(lambda x: 0 if x < 0 else x)
    data['Дефицит'] = data['Дефицит'].\
        where(
            (data['Заказ обеспечен'] == 0) &
            (data['Пометка удаления'] == 0) &
            (data['Закуп подтвержден'] == 1) &
            (data['Полная_отгрузка'] == 0) &
            (data['Некомплектная_отгрузка'] == 0),
            0
        )
    del data['Обеспечена МП']  # del data['Обеспечена МП'], data['Заказчик'], data['Спецификация']

    tn_ord = tn_orders()
    data = merge(data, tn_ord, how='left', on='Заказ-Партия', copy=False)  # индикатор ТН в таблицу потребности

    if short_term_plan is True:  # для краткосрочного планирования
        data = multiple_sort(data)  # сортировка потребности и определение
    else:
        data = data.sort_values(by='Дата запуска')  # сортировка потребности и определение

    data = data.reset_index().rename(columns={
        'index': 'Поряд_номер',
        'Документ заказа.Статус': 'Статус'
    })  # определение поряд номера

    logging.info('Потребность загрузилась')
    return data


def nomenclature() -> DataFrame:
    """Загузка таблицы со структурными данными для замен."""
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Справочник_метизов_лэп (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
        dtype={'Номенклатура.Толщина покрытия (только для ТД)': str}
    )
    rename_columns = {
        'Номенклатура.Вид номенклатуры': 'Вид',
        'Номенклатура.Марка стали (метизы только для чертежа)': 'Марка_стали',
        'Номенклатура.Толщина покрытия (только для ТД)': 'Покрытие',
        'Номенклатура.Стандарт на изделие': 'Гост',
        'Номенклатура.Класс прочности (без чертежа)': 'Класс_прочности'
    }
    data = data.rename(columns=rename_columns)

    # помещение названия в индекс
    # колонка названия номенклатуры остается и в таблице и в индексе для дальнейшей работы
    data = data.\
        rename(columns={'Номенклатура': 'index'}).\
        set_index('index', drop=False).\
        rename(columns={'index': 'Номенклатура'}). \
        fillna('')

    # Работы с покрытием
    # преобразование значений покрытия в таблице и формирование справочника
    data['Покрытие'] = modify_col(data['Покрытие'], instr=1, space=1)
    replacements_pokrit = DataFrame(
        data=data['Покрытие'][data['Покрытие'] != ''].unique(),
        columns=['Покрытие']
    )
    replacements_pokrit['Покрытие'] = replacements_pokrit['Покрытие'].map(int)
    replacements_pokrit = replacements_pokrit.sort_values(by='Покрытие')
    replacements_pokrit['Покрытие'] = 'ТД' + replacements_pokrit['Покрытие'].map(str)
    replacements_pokrit = concat([DataFrame(data=['Гл'], columns=['Покрытие']), replacements_pokrit])
    replacements_pokrit = concat([replacements_pokrit, DataFrame(data=['ГЦ'], columns=['Покрытие'])])
    replacements_pokrit.to_csv(
        r".\support_data\outloads\dict_replacement_pokrit.csv",
        sep='\t',
        encoding='ansi',
        index=False
    )
    data['Покрытие'] = data['Покрытие'].\
        where(data['Покрытие'] == '', 'ТД' + data['Покрытие']).\
        where(~data['Номенклатура'].str.contains(r'Гл|ГЛ', regex=True), 'Гл' + data['Покрытие']).\
        where(~data['Номенклатура'].str.contains(r'ГЦ|Гц', regex=True), 'ГЦ' + data['Покрытие'])

    # Работы с классом прочности
    # преобразование класса в таблице и формирование справочника
    data['Класс_прочности'] = modify_col(data['Класс_прочности'], instr=1, space=1, comma=1)
    data['Класс_прочности'] = data['Класс_прочности']. \
        where(data['Класс_прочности'] == '', data['Класс_прочности'].map(in_float))
    replacements_prochn = DataFrame(
        data=data['Класс_прочности'][data['Класс_прочности'] != ''].unique(),
        columns=['Класс_прочности']
    ).sort_values(by='Класс_прочности')
    replacements_prochn.to_csv(
        r".\support_data\outloads\dict_replacement_prochn.csv",
        sep='\t',
        encoding='ansi',
        index=False
    )

    logging.info('Номенклатура загрузилась')
    return data


def replacements(path: str) -> DataFrame:
    """Загузка таблицы с заменами или другого справочника (пути к заменам находятся в константах)

    :param path: путь к замене (файл csv)
    """
    data = read_csv(
        path,
        sep=';',
        encoding='ansi'
    )
    return data


def center_rests(dictionary: DataFrame, short_term_plan=False) -> DataFrame:
    """Загузка таблицы с остатками на центральном складе, форматирование таблицы.
    Колонка с количеством остатком должна иметь наименование "Количество".

    :param dictionary: таблица из nomenclature() - справочник номенклатуры
    :param short_term_plan: если True, то запись остаток в папку для сохранения прошлых расчетов
    """
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\!metizi (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi'
    )
    data = data.rename(columns={'Артикул': 'Код'})
    data = data[~data['Номенклатура'].isna()]
    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data = data[data['Количество'] > 0]
    data['Склад'] = 'Центральный склад'  # Склады центральные по металлу, метизам и вход контроля
    data['Дата'] = datetime(NOW.year, NOW.month, NOW.day)
    data = data.merge(dictionary, on='Номенклатура', how='left')

    if short_term_plan is True:
        data.to_csv(
            f'W:\\Analytics\\Илья\\!deficit_metiz_work_files\\rests_center_mtz {NOW.strftime("%y%m%d %H_%M_%S")}.csv',
            sep=";",
            encoding='ansi',
            index=False
        )  # запись используемых файлов, для взгляда в прошлое

    logging.info('Остатки центрального склада загрузились')
    return data


def tn_rests(dictionary: DataFrame, short_term_plan=False) -> DataFrame:
    """Загузка таблицы с остатками на центральном складе, форматирование таблицы.
    Колонка с количеством остатком должна иметь наименование "Количество".

    :param dictionary: таблица из nomenclature() - справочник номенклатуры
    :param short_term_plan: если True, то запись остаток в папку для сохранения прошлых расчетов
    """
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\!metal_tn (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
    )
    data = data.rename(columns={'Конечный остаток': "Количество", 'Артикул': 'Код'})
    data = data[~data['Номенклатура'].isna()]
    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data['Склад'] = 'ТН'
    data['Дата'] = datetime(NOW.year, NOW.month, NOW.day)
    data = data.merge(dictionary, on='Номенклатура', how='left')

    if short_term_plan is True:
        data.to_csv(
            f'W:\\Analytics\\Илья\\!deficit_metiz_work_files\\rests_tn {NOW.strftime("%y%m%d %H_%M_%S")}.csv',
            sep=";",
            encoding='ansi',
            index=False
        )  # запись используемых файлов, для взгляда в прошлое

    logging.info('Остатки склада ТН загрузились')
    return data


def future_inputs(dictionary: DataFrame, short_term_plan=False) -> DataFrame:
    """Загузка таблицы с остатками на центральном складе, форматирование таблицы.
    Колонка с количеством остатком должна иметь наименование "Количество".

    :param dictionary: таблица из nomenclature() - справочник номенклатуры
    :param short_term_plan: если True, то запись остаток в папку для сохранения прошлых расчетов
    """
    path = r"W:\Analytics\Илья\!outloads\Остатки заказов поставщикам метизы (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
        usecols=['Дата поступления', 'Номенклатура', 'Заказано остаток']
    ).rename(
        columns={'Дата поступления': 'Дата', 'Заказано остаток': 'Количество'}
    ).dropna()
    data['Дата'] = data['Дата'].map(lambda x: datetime.strptime(x, '%d.%m.%Y'))

    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data['Склад'] = 'Поступления'
    data = data.\
        fillna(0).\
        sort_values(by='Дата')
    data = data.merge(dictionary, on='Номенклатура', how='left')

    if short_term_plan is True:
        data = DataFrame(data=None, columns=list(data.columns))  # дневной дефицит без поступлений

    data.to_csv(
        r".\support_data\outloads\rest_futures_inputs.csv",
        sep=";",
        encoding='ansi',
        index=False
    )  # запись используемых файлов, для взгляда в прошлое

    logging.info('Поступления загрузились')
    return data


def tn_orders() -> DataFrame:
    """Загрузка списка заказов по ТН"""
    path = r'support_data/outloads/dict_orders_tn.txt'
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi'
    ).drop_duplicates()

    logging.info('Заказы ТН загрузилась')
    return data


def approved_orders(orders: tuple) -> DataFrame:
    """Заказы с подтверждением закупа материалов по ним.

    :param orders: список уникальных заказов
    """
    with open(r'support_data/outloads/query_approved_orders.sql') as file:
        query = file.read().format(orders)

    connection = connect(
        "Driver={SQL Server};"
        "Server=OEMZ-POBEDA;"
        "Database=ProdMgrDB;"
        "uid=1C_Exchange;pwd=1"
    )
    data = read_sql_query(query, connection)
    data['Закуп подтвержден'] = data['level_of_allowing'].map(lambda x: 1 if x >= 5 else 0)
    data['Закуп подтвержден'] = data['Закуп подтвержден'].where(
        data['number_order'].map(lambda x: False if x[1] == '0' else True),
        1
    )
    data['Возможный заказ'] = data['level_of_allowing'].map(lambda x: 1 if x == 4 else 0)
    data = data.rename(columns={'number_order': 'Номер победы'})

    return data[['Номер победы', 'Закуп подтвержден', 'Возможный заказ']]


def non_complect() -> DataFrame:
    """Загрузка списка заказов с некомплектной отгрузкой
    Если Некомплектная_отгрузка == 1, то значит эта позиция отгрузилась и в расчете не участвует
    """
    path = r'support_data/outloads/non_complect_order.csv'
    data = read_csv(
        path,
        sep=';',
        encoding='ansi',
        dtype={'Номер победы': str}
    )

    return data


def order_shipment() -> DataFrame:
    """Список отгрузок заказов
    Если Полная_отгрузка == 1, то значит эта позиция отгрузилась и в расчете не участвует
    """
    path = r"W:\Analytics\Илья\!outloads\Открузки_заказов (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
        dtype={'Номер победы': str}
    ).rename(columns={
        'Заказ пр-ва (Победа)': 'Номер победы',
        'Заказ (с учетом отмен)': 'Заказано'
    })
    data = data[~data['Договор'].isna()]
    data['Полная_отгрузка'] = 0
    data['Заказано'] = modify_col(data['Заказано'], instr=1, space=1, comma=1, numeric=1)
    data['Отгружено'] = modify_col(data['Отгружено'], instr=1, space=1, comma=1, numeric=1)
    data['Полная_отгрузка'] = data['Полная_отгрузка'].\
        where(~(data['Отгружено'] >= data['Заказано']), 1)
    data = data[['Номер победы', 'Полная_отгрузка']].drop_duplicates()

    return data
