"""Daily reports"""

from os import walk
from etl.extract import NOW, DAYS_AFTER
from pandas import (
    DataFrame, concat, read_csv, Timestamp
)
from datetime import timedelta


def daily_tables() -> None:
    """Создание таблиц для ежедневных отчетов"""
    name_output_req = r'.\support_data\output_tables\ask_{0}.csv'.format(NOW.strftime('%Y%m%d'))
    output_req = read_csv(
        name_output_req,
        sep=";",
        encoding='ansi',
        parse_dates=['Дата запуска', 'Дата начала факт'],
        dtype={'Номер победы': 'object'}
    )
    output_req.to_csv(
        f'W:\\Analytics\\Илья\\!deficit_metiz_work_files\\ask {NOW.strftime("%y%m%d %H_%M_%S")}.csv',
        sep=";",
        encoding='ansi',
        index=False
    )  # запись используемых файлов, для взгляда в прошлое
    deficit(output_req)


def deficit(table_: DataFrame) -> None:
    """Создание отчета по дефициту на сегодня + 4 дня вперед (requirements() сразу обрезает +4 дня)

    :param table_: главная таблица output_req
    """
    need_columns = [
        'Номер победы', 'Партия', 'Дата запуска',
        'Номенклатура', 'Количество в заказе',
        'Заказчик', 'Изделие', 'Остаток дефицита', 'Дата начала факт'
    ]
    table: DataFrame = table_.copy()
    # table['Остаток дефицита'] = table['Остаток дефицита'] + table['Списание из Поступлений']
    table['Заказчик'] = table['Заказчик'].replace({0: 'Омский ЭМЗ', '0': 'Омский ЭМЗ'})
    table = table[(table['Заказ обеспечен'] == 0) & (table['Пометка удаления'] == 0)]
    table = table[need_columns]

    first_table = main_deficit_table(table)
    second_table = second_deficit_table(
        table_=first_table,
        status_table=table_.loc[:, ['Номер победы', 'Партия', 'Статус']].drop_duplicates()
    )
    first_table['Дата запуска ФАКТ'] = first_table['Дата запуска ФАКТ'].replace({'0': None})

    first_table.to_csv(
        r'.\support_data\data_for_reports\daily_deficit_1.csv',
        sep=";",
        encoding='ansi',
        index=False
    )
    second_table.to_csv(
        r'.\support_data\data_for_reports\daily_deficit_2.csv',
        sep=";",
        encoding='ansi',
        index=False
    )

    provided = provided_table(table)
    provided.to_csv(
        r'.\support_data\data_for_reports\provided.csv',
        sep=";",
        encoding='ansi',
        index=False
    )


def main_deficit_table(table: DataFrame) -> DataFrame:
    """Создание заготовки главной таблицы ежедневного отчета по дефициту

    :param table: таблица с подготовленными данными output_req из deficit()
    """
    group_columns = [
        'Номер победы', 'Партия', 'Дата запуска',
        'Заказчик', 'Изделие', 'Дата начала факт'
    ]
    problems = compare_with_prev_ask(table)  # готовая таблица с индикатором проблем (переносы, отклонение потреб)

    detail_table = table.copy()
    detail_table = detail_table.groupby(by=group_columns).sum().reset_index()
    detail_table['Проблема'] = None
    detail_table['Обеспеченность'] = 1 - (detail_table['Остаток дефицита'] / detail_table['Количество в заказе'])
    detail_table['Остаточная потребность'] = None
    detail_table['Дата запуска ФАКТ'] = detail_table['Дата начала факт']
    del detail_table['Дата начала факт']
    detail_table = detail_table[detail_table['Остаток дефицита'] >= 0.01]
    detail_table = detail_table.sort_values(by=['Дата запуска'])

    first_table = list()
    for i in range(len(detail_table)):  # заполнение первой таблицы отчета
        row = detail_table.iloc[i]
        first_table.append(row.to_list())

        nomenclature_row = table[
            (table['Номер победы'] == row['Номер победы']) &
            (table['Партия'] == row['Партия']) &
            (table['Остаток дефицита'] > 0)
        ].copy()
        nomenclature_row['Заказчик'] = nomenclature_row['Номенклатура']
        nomenclature_row['Остаточная потребность'] = nomenclature_row['Остаток дефицита']

        nomenclature_row = nomenclature_row.merge(
            problems,
            how='left',
            on=['Номер победы', 'Партия', 'Номенклатура']
        )

        nomenclature_row['Обеспеченность'] = None
        nomenclature_row['Дата запуска ФАКТ'] = None
        row_columns = set(table.columns) - {'Заказчик', 'Остаточная потребность'}
        nomenclature_row[list(row_columns)] = None
        nomenclature_row = nomenclature_row[detail_table.columns]
        for ii in range(len(nomenclature_row)):
            first_table.append(nomenclature_row.iloc[ii].to_list())

    # работа с колонками
    first_table = DataFrame(data=first_table, columns=detail_table.columns)
    first_table = first_table[[
        'Дата запуска', 'Дата запуска ФАКТ', 'Заказчик',
        'Изделие', 'Номер победы', 'Партия',
        'Остаточная потребность', 'Обеспеченность', 'Проблема'
    ]]
    first_table = first_table.rename(columns={
        'Дата запуска': 'Дата запуска ПЛАН',
        'Заказчик': 'Заказчик/Сортамент',
        'Номер победы': '№ заказа'
    })
    first_table['Дата закрытия дефицита'] = None
    first_table['Примечание МТО'] = None
    first_table['Примечание ПО'] = None
    return first_table


def compare_with_prev_ask(table: DataFrame) -> DataFrame:
    """Загружает предпредыдущий файл недельного расчета.
    И сравнивает с текущим расчетом по дате запуска и кол-ву в заказе.

    :param table: таблица с подготовленными данными output_req из deficit()
    """
    end_date = NOW + timedelta(days=DAYS_AFTER)
    path = r'W:\Analytics\Илья\Задание 14 Расчет потребности для МТО\data_metiz'
    need_columns = ['Номер победы', 'Партия', 'Дата запуска', 'Номенклатура', 'Количество в заказе']

    name_prev_file = [i for i in walk(path)][0][2][-1]
    path += "\\" + name_prev_file
    prev_data = read_csv(
        path,
        sep=";",
        encoding='ansi',
        parse_dates=['Дата запуска']
    )
    prev_data = prev_data[prev_data['Дата запуска'] <= end_date][need_columns]

    data: DataFrame = table.copy()[need_columns]
    data = data.merge(
        prev_data,
        how='left',
        on=['Номер победы', 'Партия', 'Номенклатура'],
        suffixes=('_cur', '_prev'))

    # вычисляемые столбцы
    data['Проблема'] = None
    data['Проблема'] = (
        'Изменение даты ' + '(' +
        data['Дата запуска_prev'].map(lambda x: x.strftime('%d.%m.%Y') if isinstance(x, Timestamp) else str(x)) +
        '->' + data['Дата запуска_cur'].map(lambda x: x.strftime('%d.%m.%Y')) + ')'
    ).where(
        (data['Дата запуска_cur'] != data['Дата запуска_prev']) &
        ~(data['Дата запуска_prev'].isna()),
        data['Проблема']
    )
    data['Проблема'] = data['Проблема'].where(
        ~(data['Дата запуска_prev'].isna()),
        'Не было в этом периоде'
    )
    data['Проблема'] = (
        'Изменение потребности (' +
        data['Количество в заказе_prev'].map(lambda x: str(round(x, 3))) + '->' +
        data['Количество в заказе_cur'].map(lambda x: str(round(x, 3))) + ')'
    ).where(
        (data['Количество в заказе_cur'] > data['Количество в заказе_prev']),
        data['Проблема']
    )
    data = data[['Номер победы', 'Партия', 'Номенклатура', 'Проблема']]
    return data.drop_duplicates()


def second_deficit_table(table_: DataFrame, status_table: DataFrame) -> DataFrame:
    """Создание второй таблицы ежедневного отчета по дефициту

    :param table_: таблица из main_deficit_table
    :param status_table: главная таблица output_req с номерами заказов и статусом
    """
    table = table_.copy()
    table = table.merge(
        status_table,
        left_on=['№ заказа', 'Партия'],
        right_on=['Номер победы', 'Партия'],
        how='left'
    )
    table['Дата запуска ПЛАН'] = table['Дата запуска ПЛАН'].ffill()
    table['Дата запуска ФАКТ'] = table['Дата запуска ФАКТ'].ffill()
    table['Статус'] = table['Статус'].ffill()

    closed = table[table['Статус'] == 'Закрыт']
    launch = table[table['Дата запуска ФАКТ'] != '0']
    non_launch = table[table['Дата запуска ФАКТ'] == '0']

    need_columns = ['Заказчик/Сортамент', 'Остаточная потребность']
    closed = closed[need_columns][closed['Изделие'].isna()]
    launch = launch[need_columns][launch['Изделие'].isna()]
    non_launch = non_launch[need_columns][non_launch['Изделие'].isna()]

    closed = closed.\
        groupby(by=['Заказчик/Сортамент']).\
        sum().\
        reset_index()
    launch = launch.\
        groupby(by=['Заказчик/Сортамент']).\
        sum().\
        reset_index()
    non_launch = non_launch.\
        groupby(by=['Заказчик/Сортамент']).\
        sum().\
        reset_index()

    second_table = DataFrame(data=None, columns=launch.columns)

    second_table.loc[0] = ['Заказ закрыт по про-ву', closed['Остаточная потребность'].sum()]
    second_table = concat([second_table, closed])

    second_table.loc[len(second_table)] = ['В работе', launch['Остаточная потребность'].sum()]
    second_table = concat([second_table, launch])

    second_table.loc[len(second_table)] = ['Не в работе', non_launch['Остаточная потребность'].sum()]
    second_table = concat([second_table, non_launch])

    sum_deficit = second_table['Остаточная потребность'][
        second_table['Заказчик/Сортамент'].isin(['Заказ закрыт по про-ву', 'В работе', 'Не в работе'])
    ].sum()
    second_table.loc[len(second_table)] = ['ИТОГО', sum_deficit]

    second_table.columns = ['Номенклатура металла', 'Потребность']
    second_table['Комментарий МТО'] = None
    return second_table


def provided_table(table: DataFrame) -> DataFrame:
    """Создание таблицы с полностью обеспеченными заказами из склада

    :param table: таблица с подготовленными данными output_req из deficit()
    """
    group_columns = [
        'Номер победы', 'Партия', 'Дата запуска',
        'Заказчик', 'Изделие'
    ]
    prov_table = table.copy()
    prov_table = prov_table.groupby(by=group_columns).sum().reset_index()
    prov_table['Обеспеченность'] = 1 - (prov_table['Остаток дефицита'] / prov_table['Количество в заказе'])
    prov_table['Дата запуска ФАКТ'] = None  # потом заменится на существующую колонку
    prov_table = prov_table[prov_table['Остаток дефицита'] < 0.0001]
    prov_table = prov_table.sort_values(by=['Дата запуска'])

    # работа с колонками
    prov_table = prov_table[[
        'Дата запуска', 'Дата запуска ФАКТ',
        'Заказчик', 'Изделие', 'Номер победы',
        'Партия', 'Обеспеченность'
    ]]
    prov_table = prov_table.rename(columns={
        'Дата запуска': 'Дата запуска ПЛАН',
        'Заказчик': 'Заказчик/Сортамент',
        'Номер победы': '№ заказа'
    })
    prov_table['Обеспеченность'] = prov_table['Обеспеченность'].replace({None: 1})  # заказы с 0 потребностью, но с перемещениями
    return prov_table
