"""Search nomenclature analog
Поиск идентичной или аналогичной номенклатуры.
Алогритм как write_off"""

from pandas import (DataFrame, Series, concat)


def building_purchase_analysis(
    table: DataFrame,
    orders: DataFrame,
    nom_: DataFrame,
    repl_: dict
) -> None:
    """Процесс списания остатков и создания файлов csv

    :param table: таблица потребностей из итогового отчета
    :param orders: данные о закупках менеджеров
    :param nom_: справочник номенклатуры
    :param repl_: словарь со справочниками замен {
            'mark': dict_repl_mark,
            'gost': dict_repl_gost,
            'pokr': dict_repl_pokr,
            'prochn': dict_repl_prochn
        }
    """
    # сначала мержим и делаем тем самым поиск идентичной номенклатуры
    data = table.\
        copy().\
        merge(orders, on='Номенклатура', how='left')

    # потом только в несмерженных данных производим поиск
    index_rests_nom = data[
        data['Номенклатура'].isin(set(data['Номенклатура']) - set(orders['Номенклатура']))].\
        index
    copy_orders = orders[
        orders['Номенклатура'].isin(set(orders['Номенклатура']) - set(data['Номенклатура']))].\
        copy()

    for i in index_rests_nom:
        if len(copy_orders) == 0:
            break
        replacement(
            ind=i,
            sklad=copy_orders,
            table=data,
            nom_=nom_,
            repl_=repl_
        )
        copy_orders = copy_orders.dropna()

    copy_orders['Дефицит'] = 0
    copy_orders = copy_orders[[
        'Номенклатура', 'Дефицит', 'Заказано', 'Доставлено'
    ]]
    data = concat([data, copy_orders], axis=0)
    data = data.\
        fillna(0).\
        sort_values(by='Номенклатура')

    data['Еще_заказать'] = data['Дефицит'] - data['Заказано']
    data.to_excel(
        r".\support_data\purchase_analysis\purchase_analysis.xlsx",
        index=False
    )


def replacement(
    ind: int,
    sklad: DataFrame,
    table: DataFrame,
    nom_: DataFrame,
    repl_: dict
) -> None:
    """Поиск аналогичной номенклатуры для анализа закупа менеджеров

    :param ind: индекс строчки в таблице потребностей
    :param sklad: данные о закупках менеджеров
    :param table: таблица потребностей
    :param nom_: справочник номенклатуры
    :param repl_: словарь со справочниками замен {
            'mark': dict_repl_mark,
            'gost': dict_repl_gost,
            'pokr': dict_repl_pokr,
            'prochn': dict_repl_prochn
        }
    """
    cur_nom = table.at[ind, 'Номенклатура']
    if len(nom_[nom_['Номенклатура'] == cur_nom]) == 0:
        return None  # быстрый выход из списания, если cur_nom нет в справочнике номенклатур

    # определение опурядоченного списка замен
    need_replacements = search_replacements(
        cur_nom=cur_nom,
        sklad=sklad,
        dict_nom=nom_,
        dict_repl=repl_
    )
    if len(need_replacements) == 0:
        return None
    else:
        table.at[ind, 'Заказано'] = need_replacements['Заказано'].sum()
        sklad['Заказано'] = sklad['Заказано'].\
            where(~sklad['Номенклатура'].isin(need_replacements['Номенклатура']), None)

        table.at[ind, 'Доставлено'] = need_replacements['Доставлено'].sum()
        sklad['Доставлено'] = sklad['Доставлено'].\
            where(~sklad['Номенклатура'].isin(need_replacements['Номенклатура']), None)


def search_replacements(
    cur_nom: str,
    sklad: DataFrame,
    dict_nom: DataFrame,
    dict_repl: dict
) -> DataFrame:
    """Поиск взаимозамен по нескольким параметрам из словарь со справочниками замен

    :param cur_nom: текущая номенклатура
    :param sklad: заказы поставщикам
    :param dict_nom: справочник номенклатур
    :param dict_repl:
    :return: словарь со справочниками замен {
            'mark': dict_repl_mark,
            'gost': dict_repl_gost,
            'pokr': dict_repl_pokr,
            'prochn': dict_repl_prochn
        }
    """
    sklad_ = sklad\
        [sklad['Номенклатура'].str.startswith(cur_nom.split()[0])].\
        copy()
    sklad_ = sklad_.merge(dict_nom, how='left', on='Номенклатура')

    # поиск возможных замен среди атрибутов (гост, покрытие, марка, прочность)
    # для приоритезации внутри найденных элементов замен используется индекс, он уже в прав порядке

    # cur_mark
    cur_mark = dict_nom.at[cur_nom, 'Марка_стали']
    try:  # если не нашел такубю марку-категорию, то ищет cur_mark
        repl_marks = dict_repl['mark'].loc[:, cur_mark]
        repl_marks = repl_marks[~repl_marks.isna()]
    except KeyError:
        repl_marks = Series(data=cur_mark)

    # cur_gost
    cur_gost = dict_nom.at[cur_nom, 'Гост']
    try:  # если не нашел такой гост, то ищет cur_gost
        repl_gosts = dict_repl['gost'].loc[:, cur_gost]
        repl_gosts = repl_gosts[~repl_gosts.isna()]
    except KeyError:
        repl_gosts = Series(data=cur_gost)

    # cur_pokr
    cur_pokr = dict_nom.at[cur_nom, 'Покрытие']
    if cur_pokr == '':
        repl_pokrs = Series(data=cur_pokr)
    else:
        i_cur_pokr = dict_repl['pokr'][dict_repl['pokr']['Покрытие'] == cur_pokr].index[0]
        repl_pokrs = dict_repl['pokr'].iloc[i_cur_pokr:, 0]

    # cur_prochn
    cur_prochn = dict_nom.at[cur_nom, 'Класс_прочности']
    if cur_prochn == '':
        repl_prochns = Series(data=cur_prochn)
    else:
        i_cur_prochn = dict_repl['prochn'][dict_repl['prochn']['Класс_прочности'] == cur_prochn].index[0]
        repl_prochns = dict_repl['prochn'].iloc[i_cur_prochn:, 0]

    # созданеи паттерна поиска, которые не содержит атрибуты (гост, марка, прочность, покрытие)
    pattern = cur_nom
    if cur_mark != '':  # если в списке номенклатур марка = пустое значение
        pattern = pattern.replace(str(cur_mark), '.+')
    if cur_gost != '':
        pattern = pattern.replace(str(cur_gost), '.+')
    if cur_pokr != '':
        pattern = pattern.replace(str(cur_pokr), '.+')
    if cur_prochn != '':
        pattern = pattern.replace(str(cur_prochn), '.+')

    sklad_ = sklad_[
        sklad_['Номенклатура'].str.contains(pattern, regex=True) &
        sklad_['Марка_стали'].isin(repl_marks) &
        sklad_['Гост'].isin(repl_gosts) &
        sklad_['Покрытие'].isin(repl_pokrs) &
        sklad_['Класс_прочности'].isin(repl_prochns)
    ]

    # правильная сортировка order = порядок
    # только если длина найденных номенклатур больше 1
    if len(sklad_) > 1:
        order_mark = DataFrame(data=repl_marks.values, columns=['Марка_стали'])
        order_mark['order_mark'] = order_mark.index

        order_gost = DataFrame(data=repl_gosts.values, columns=['Гост'])
        order_gost['order_gost'] = order_gost.index

        order_pokr = DataFrame(data=repl_pokrs.values, columns=['Покрытие'])
        order_pokr['order_pokr'] = order_pokr.index

        order_prochn = DataFrame(data=repl_prochns.values, columns=['Класс_прочности'])
        order_prochn['order_prochn'] = order_prochn.index

        INDEX = sklad_.index
        sklad_ = sklad_.\
            merge(order_mark, on='Марка_стали', how='left').\
            merge(order_gost, on='Гост', how='left').\
            merge(order_pokr, on='Покрытие', how='left').\
            merge(order_prochn, on='Класс_прочности', how='left')
        sklad_.index = INDEX  # после мержа восстанавливаем индекс
        sklad_ = sklad_.sort_values(by=[
            'order_gost', 'order_pokr', 'order_prochn', 'order_mark'
        ])
    return sklad_
