"""Weekly reports"""

from common.common import extract_day
from etl.extract import NOW, long_term_sortaments
from pandas import (
    DataFrame, pivot_table, Series, concat
)
from datetime import datetime


def weekly_tables(
        start_ask_: DataFrame, 
        end_ask_: DataFrame,
        oper_: list,
        sep_date: datetime = None
) -> None:
    """Подготовка таблиц для недельных отчетов

    :param start_ask_: начальная поребность
    :param end_ask_: конечная поребность после списаний
    :param oper_: список операций
    :param sep_date: Дата разделения краткосрочного периода и долгосрочного = последний день краткосрочного
    """
    need_columns = [
        'Заказ-Партия', 'Номенклатура потребности',
        'Склад', 'Списание потребности'
    ]
    group_columns = [
        'Заказ-Партия', 'Номенклатура потребности',
        'Склад'
    ]

    # oper_write_off - укороченная версия operations_table для мержа к output_req
    oper_write_off = operations_table(oper_)
    oper_write_off = oper_write_off[need_columns].groupby(group_columns).sum().reset_index()

    center_write_off = write_off_tables(
        table_=oper_write_off, 
        from_='Центральный склад'
    )
    tn_write_off = write_off_tables(
        table_=oper_write_off,
        from_='ТН'
    )
    future_input_write_off = write_off_tables(
        table_=oper_write_off,
        from_='Поступления'
    )

    #  output_req - это start_ask с изменениями для вывода в файл
    output_req = main_table(
        start_ask_=start_ask_,
        end_ask_=end_ask_,
        list_tables=[center_write_off, tn_write_off, future_input_write_off]
    )

    name_output_req = r'.\support_data\output_tables\ask_{0}.csv'.format(NOW.strftime('%Y%m%d'))
    output_req.to_csv(name_output_req, sep=";", encoding='ansi', index=False)

    # создание файлов для макроса экселя
    # detail_table - таблица для краткосрочного закупа
    if sep_date is None:  # если None, то для дефицита ежедневного и далее ну нужно идти
        return None

    make_detail_table(data=output_req, sep_date=sep_date)

    # графики подготавливаются только для короткого периода
    short_term = output_req[output_req['Дата запуска'] <= sep_date]
    graph(table_=short_term, method='with_future_inputs')
    graph(table_=short_term, method='without_future_inputs')

    # подготовка неподтвержденных краткосрочных заказов
    unpr_orders = make_unapproved_orders(data=output_req, sep_date=sep_date)

    # подготовка неподтвержденных долгосрочных заказов с длинной номенклатурой
    unpr_long_orders = make_unapproved_long_orders(data=output_req, sep_date=sep_date)

    problem_orders = concat([unpr_orders, unpr_long_orders], axis=0)
    problem_orders.to_csv(
        r".\support_data\data_for_reports\problem_orders.csv",
        sep=";", encoding='ansi', index=False
    )
    # подготовка дефицита длинной номенклатуры в долгосрочной перспективе
    long_nomenclature_orders(data=output_req, sep_date=sep_date)


def operations_table(oper_: list) -> DataFrame:
    """
    Главная задача функции - установка даты учета на основании дат заказа или дат поступлений
    Если списание не из поступлений, то 'Дата учета' = 'Дата списания остат'
    Если списание из поступлений и 'Дата потребности' > 'Дата списания остат',
        то 'Дата учета' = 'Дата потребности'
    Если списание из поступлений и 'Дата потребности' <= 'Дата списания остат',
        то 'Дата учета' = 'Дата списания остат'
    Так же сразу записываем файл с операциями.

    :param oper_: список операций
    :return: отформатированная таблица с операциями
    """
    columns = [
        'Дата потребности', 'Порядковый номер', 
        'Заказ-Партия', 'Номенклатура потребности', 
        'Потребность из файла', 'Потребность нач', 
        'Потребность кон', 'Списание потребности', 
        'Склад', 'Дата списания остат', 'Номенклатура Списания', 
        'Остатки нач', 'Остатки кон', 'Списание остатков'
    ]
    table = DataFrame(data=oper_, columns=columns)
    table['Дата учета'] = table['Дата потребности'].where(
        table['Склад'] != 'Поступления', table['Дата списания остат']
    )
    table['Дата учета'] = table['Дата потребности'].where(
        (table['Склад'] == 'Поступления') &
        (table['Дата потребности'] > table['Дата списания остат']),
        table['Дата учета']
    )
    name_oper = r'.\support_data\output_tables\oper_{0}.csv'.format(NOW.strftime('%Y%m%d'))
    table.to_csv(name_oper, sep=";", encoding='ansi', index=False)
    
    return table


def write_off_tables(table_: DataFrame, from_: str) -> DataFrame:
    """
    Создает таблицу списаний из нужного источника.

    :param table_: таблица из oper_write_off
    :param from_: из какого источника списание ('Центральный склад', 'ТН', 'Поступления')
    :return: 3 таблицы со списаниями из разных источников
    """
    name_write_off_column = {
        'Центральный склад': 'Списание из Цент склада',
        'ТН': 'Списание из ТН',
        'Поступления': 'Списание из Поступлений'
    }
    if from_ in name_write_off_column.keys():
        write_off = table_[table_['Склад'] == from_][[
            'Заказ-Партия', 'Номенклатура потребности',
            'Списание потребности'
        ]].rename(
            columns={
                'Номенклатура потребности': 'Номенклатура',
                'Списание потребности': name_write_off_column[from_]
            }
        )
        write_off['Заказ обеспечен'] = 0
        write_off['Пометка удаления'] = 0
        return write_off
    else:
        raise AttributeError(
            "Argument 'from_' receives only 3 values: Центральный склад', 'ТН', 'Поступления'"
        )


def main_table(
        start_ask_: DataFrame, 
        end_ask_: DataFrame,
        list_tables: list,
) -> DataFrame:
    """
    Мержит в главную таблицу списания из разных источников в колонки

    :param start_ask_: изначальная таблица start_ask
    :param end_ask_: таблица end_ask после списаний
    :param list_tables: список таблиц которые надо смержить с главной таблицей
                        это таблицы списаний из разных источников
    :return: главная таблица со всей информацией 'ask_20200131.csv'
    """
    data = start_ask_

    for t in list_tables:
        data = data.merge(
            t,
            left_on=['Заказ-Партия', 'Номенклатура', 'Заказ обеспечен', 'Пометка удаления'],
            right_on=['Заказ-Партия', 'Номенклатура', 'Заказ обеспечен', 'Пометка удаления'],
            how='outer', copy=False
        )
        
    data = data.fillna(0)
    data = data.merge(
        end_ask_[['Поряд_номер', 'Дефицит']].rename(
            columns={'Дефицит': 'Остаток дефицита'}
        ),
        on='Поряд_номер',
        how='left',
        copy=False
    )

    return data


def graph(table_: DataFrame, method: str) -> None:
    """Создание графика дефицита по дням и по номенклатуре для
    
    :param table_: таблица output_req из weekly_tables
    :param method: 'with_future_inputs' or 'without_future_inputs'
    """
    if method == 'with_future_inputs':
        need_table = table_.copy()
        name_combin_graph = r'.\support_data\output_tables\graf_{0}.csv'.format(NOW.strftime('%Y%m%d'))
        name_combin_graph_excel = r".\support_data\data_for_reports\graf.csv"
    elif method == 'without_future_inputs':
        need_table = table_.copy()
        need_table['Остаток дефицита'] = need_table['Остаток дефицита'] + need_table['Списание из Поступлений']
        name_combin_graph = r'.\support_data\output_tables\graf_without_feat_{0}.csv'.format(NOW.strftime('%Y%m%d'))
        name_combin_graph_excel = r".\support_data\data_for_reports\graf_without_feat.csv"
    else:
        raise AttributeError(
            'Argument "method" receives only 2 values: with_future_inputs or without_future_inputs'
        )

    # clean_for_graf - output_req только с потребностями больше 0
    clean_for_graf = need_table[
        (need_table['Остаток дефицита'] > 0) &
        (need_table['Заказ обеспечен'] == 0) &
        (need_table['Пометка удаления'] == 0)
        ]

    # graph_ - график-календарь по дням и по номенклатурам из поребностей
    graph_ = pivot_table(
        data=clean_for_graf, 
        values='Остаток дефицита', 
        columns='Дата запуска',
        index='Номенклатура', 
        aggfunc='sum'
    ).fillna(0).sort_index()

    # создание comdin_graf файла
    need_date = Series(graph_.columns)
    need_date = need_date[need_date >= NOW]
    cum_column = graph_[graph_.columns[graph_.columns < NOW]].sum(axis=1)  # столбец с кумулятивными данными предыдущих дней

    combin_graph = graph_[need_date].copy()
    combin_graph[need_date.iloc[0]] = cum_column + combin_graph[need_date.iloc[0]]

    # создание мультииндекса, где верхний уровень отклонение от первого дня
    columns = Series(combin_graph.columns).diff().map(extract_day).cumsum().replace({None: 0})
    combin_graph.columns = [columns, combin_graph.columns]

    filter1 = combin_graph.sum(axis=1).replace({0: None}).notna()
    
    combin_graph[filter1].to_csv(name_combin_graph, sep=";", encoding='ansi')
    combin_graph[filter1].to_csv(name_combin_graph_excel, sep=";", encoding='ansi')


def make_detail_table(data: DataFrame, sep_date: datetime) -> None:
    """Подготавливает таблицу для еженедельного отчета

    :param data: output_req
    :param sep_date: Дата разделения краткосрочного периода и долгосрочного = последний день краткосрочного
    """
    detail_table = data[[
        'Дата запуска', 'Заказ-Партия', 'Заказчик',
        'Спецификация', 'Номенклатура', 'Дефицит',
        'Остаток дефицита', 'Списание из Цент склада',
        'Списание из ТН', 'Списание из Поступлений', 'ТН'
    ]][
        (data['Заказ обеспечен'] == 0) &
        (data['Пометка удаления'] == 0) &
        (data['Дефицит'] != 0) &
        (data['Дата запуска'] <= sep_date)
    ]  # .sort_values(by=['Дата запуска', 'Заказ-Партия']).copy()  таблица и так отсортирована

    detail_table.to_csv(
        r".\support_data\data_for_reports\detail.csv",
        sep=";", encoding='ansi', index=False
    )


def make_unapproved_orders(data: DataFrame, sep_date: datetime) -> DataFrame:
    """Подготавливает таблицу с неподтвержденными к закупу заказами в краткосрочном периоде

    :param data: output_req
    :param sep_date: Дата разделения краткосрочного периода и долгосрочного = последний день краткосрочного
    """
    unapproved_orders = data[
        (data['Заказ обеспечен'] == 0) &
        (data['Пометка удаления'] == 0) &
        (data['Закуп подтвержден'] == 0) &
        (data['Количество в заказе'] != 0) &
        (data['Дата запуска'] <= sep_date)
        ][[
        'Дата запуска', 'Заказ-Партия', 'Заказчик',
        'Спецификация', 'Номенклатура',
        'Количество в заказе', 'Перемещено'
    ]]
    unapproved_orders['Количество в заказе'] = (
            unapproved_orders['Количество в заказе'] - unapproved_orders['Перемещено']
    )
    del unapproved_orders['Перемещено']
    unapproved_orders = unapproved_orders.rename(columns={'Количество в заказе': 'К обеспечению'})
    unapproved_orders['Комментарий'] = 'Заказы без подтверждения закупа в краткосрочной перспективе'

    return unapproved_orders


def make_unapproved_long_orders(data: DataFrame, sep_date: datetime) -> DataFrame:
    """Подготавливает таблицу с неподтвержденными к закупу заказами в краткосрочном периоде

    :param data: output_req
    :param sep_date: Дата разделения краткосрочного периода и долгосрочного = последний день краткосрочного
    """
    lts = long_term_sortaments()
    unapproved_long_orders = data[
        (data['Заказ обеспечен'] == 0) &
        (data['Пометка удаления'] == 0) &
        (data['Закуп подтвержден'] == 0) &
        (data['Количество в заказе'] != 0) &
        (data['Дата запуска'] > sep_date)
    ].copy()
    unapproved_long_orders['Длинная_ном'] = unapproved_long_orders['Номенклатура'].map(
        lambda x: check_long_nomenclature(x, lts)
    )
    unapproved_long_orders = unapproved_long_orders[
        (unapproved_long_orders['Длинная_ном'] == 1)
    ]
    unapproved_long_orders = unapproved_long_orders[[
        'Дата запуска', 'Заказ-Партия', 'Заказчик',
        'Спецификация', 'Номенклатура',
        'Количество в заказе', 'Перемещено'
    ]]
    unapproved_long_orders['Количество в заказе'] = (
            unapproved_long_orders['Количество в заказе'] - unapproved_long_orders['Перемещено']
    )
    del unapproved_long_orders['Перемещено']
    unapproved_long_orders = unapproved_long_orders.rename(columns={'Количество в заказе': 'К обеспечению'})
    unapproved_long_orders['Комментарий'] = 'Заказы без подтверждения закупа с длинной номенклатурой'

    return unapproved_long_orders


def check_long_nomenclature(x: str, list_long_sort: DataFrame) -> int:
    """Проверяет является ли номенклатура длинной, считает очень долго"""
    list_long_sort = list(list_long_sort['Сортамент'].copy().values)
    for sortam in list_long_sort:
        if sortam in str(x):
            return 1
    return 0


def long_nomenclature_orders(data: DataFrame, sep_date: datetime) -> None:
    """Подготавливает 2 таблиц: с заказами в долгосрочной перспективе с длинной номенклатурой,
        и с заказами и длинной номенклатурой, но без подтверждения закупа (статус 4)

    :param data: output_req
    :param sep_date: Дата разделения краткосрочного периода и долгосрочного = последний день краткосрочного
    """
    lts = long_term_sortaments()
    lmo = data[
        (data['Заказ обеспечен'] == 0) &
        (data['Пометка удаления'] == 0) &
        (data['Закуп подтвержден'] == 1) &
        (data['Дефицит'] != 0) &
        (data['Дата запуска'] > sep_date)
    ].copy()
    lmo['Длинная_ном'] = lmo['Номенклатура'].map(lambda x: check_long_nomenclature(x, lts))
    lmo = lmo[(lmo['Длинная_ном'] == 1)]
    lmo = lmo[[
        'Дата запуска', 'Заказ-Партия', 'Заказчик',
        'Спецификация', 'Номенклатура', 'Дефицит'
    ]]

    lmo.to_csv(
        r".\support_data\data_for_reports\long_nomenclature_orders.csv",
        sep=";", encoding='ansi', index=False
    )

    # подготовка длинной номенклатуры со статусом 4
    possible = data[
        (data['Заказ обеспечен'] == 0) &
        (data['Пометка удаления'] == 0) &
        (data['Возможный заказ'] == 1) &
        (data['Количество в заказе'] != 0) &
        (data['Дата запуска'] > sep_date)
    ].copy()
    possible['Дефицит'] = possible['Количество в заказе'] - possible['Перемещено']
    possible['Длинная_ном'] = possible['Номенклатура'].map(lambda x: check_long_nomenclature(x, lts))
    possible = possible[(possible['Длинная_ном'] == 1) & (possible['Дефицит'] != 0)]
    possible = possible[[
        'Дата запуска', 'Заказ-Партия', 'Заказчик',
        'Спецификация', 'Номенклатура', 'Дефицит'
    ]]

    possible.to_csv(
        r".\support_data\data_for_reports\long_nomenclature_possible_orders.csv",
        sep=";", encoding='ansi', index=False
    )
