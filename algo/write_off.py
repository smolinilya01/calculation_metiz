"""Write off"""

from pandas import (DataFrame, Series)


def write_off(
    table: DataFrame,
    rest_tn: DataFrame,
    rest_c: DataFrame,
    fut: DataFrame,
    oper_: list,
    nom_: DataFrame,
    repl_: dict
) -> (DataFrame, DataFrame, DataFrame, DataFrame, list):
    """Процесс списания остатков и создания файлов csv

    :param table: таблица потребностей end_ask
    :param rest_tn: остатки ТН
    :param rest_c: остатки центральных складов
    :param fut: поступления
    :param oper_: таблица со списком операция списания
    :param nom_: справочник номенклатуры
    :param repl_: словарь со справочниками замен {
            'mark': dict_repl_mark,
            'gost': dict_repl_gost,
            'pokr': dict_repl_pokr,
            'prochn': dict_repl_prochn
        }
    """
    index_clean_start_ask = table[
        (table['Дефицит'] > 0) &
        (table['Заказ обеспечен'] == 0) &
        (table['Пометка удаления'] == 0)
    ].index  # индексы по которым нужно пройтись

    for i in index_clean_start_ask:
        if table.at[i, 'ТН'] == 1:  # если заказ от ТН
            original(i, rest_tn, table, oper_)
            replacement(i, rest_tn, table, oper_, nom_, repl_)
            original(i, rest_c, table, oper_)
            replacement(i, rest_c, table, oper_, nom_, repl_)
            original(i, fut, table, oper_)
            replacement(i, fut, table, oper_, nom_, repl_)
        else:
            original(i, rest_c, table, oper_)
            replacement(i, rest_c, table, oper_, nom_, repl_)
            original(i, fut, table, oper_)
            replacement(i, fut, table, oper_, nom_, repl_)

    oper_ = DataFrame(oper_)
    return table, rest_tn, rest_c, fut, oper_


def original(
    ind: int,
    sklad: DataFrame,
    table: DataFrame,
    oper_: list
) -> None:
    """Списание необходимой номенклатуры со склада

    :param ind: индекс строчки в таблице потребностей
    :param sklad: склад списания
    :param table: таблица потребностей end_ask
    :param oper_: список операций
    """
    if table.at[ind, 'Дефицит'] == 0:
        return None  # быстрый выход из списания, если потребность = 0

    cur_nom = table.at[ind, 'Номенклатура']
    index_rests = sklad[sklad['Номенклатура'] == cur_nom].sort_values(by='Дата').index

    for i_row in index_rests:  # i_row - это индекс найденных строчек в остатках по датам
        row_ask = table.loc[ind].copy()
        ask_start = row_ask['Дефицит']
        row_rest = sklad.loc[i_row].copy()
        rest_nom_start = row_rest['Количество']

        if rest_nom_start == 0:
            pass
        else:
            if (rest_nom_start - ask_start) < 0:  # если не полность покрывается остатком
                row_rest['Количество'] = 0
                sklad.loc[i_row] = row_rest
                row_ask['Дефицит'] = ask_start - rest_nom_start
                table.loc[ind] = row_ask
                row_for_oper = {
                    'Дата потребности': row_ask['Дата запуска'],
                    'Порядковый номер': row_ask['Поряд_номер'],
                    'Заказ-Партия': row_ask['Заказ-Партия'],
                    'Номенклатура потребности': cur_nom,
                    'Потребность из файла': row_ask['Дефицит'],
                    'Потребность нач': ask_start,
                    'Потребность кон': ask_start - rest_nom_start,
                    'Списание потребности': rest_nom_start,
                    'Склад': row_rest['Склад'],
                    'Дата списания остат': row_rest['Дата'],
                    'Номенклатура Списания': row_rest['Номенклатура'],
                    'Остатки нач': rest_nom_start,
                    'Остатки кон': 0,
                    'Списание остатков': rest_nom_start
                }
                oper_.append(row_for_oper)
            else:
                row_rest['Количество'] = rest_nom_start - ask_start
                sklad.loc[i_row] = row_rest
                row_ask['Дефицит'] = 0
                table.loc[ind] = row_ask
                row_for_oper = {
                    'Дата потребности': row_ask['Дата запуска'],
                    'Порядковый номер': row_ask['Поряд_номер'],
                    'Заказ-Партия': row_ask['Заказ-Партия'],
                    'Номенклатура потребности': cur_nom,
                    'Потребность из файла': row_ask['Дефицит'],
                    'Потребность нач': ask_start,
                    'Потребность кон': 0,
                    'Списание потребности': ask_start,
                    'Склад': row_rest['Склад'],
                    'Дата списания остат': row_rest['Дата'],
                    'Номенклатура Списания': row_rest['Номенклатура'],
                    'Остатки нач': rest_nom_start,
                    'Остатки кон': rest_nom_start - ask_start,
                    'Списание остатков': ask_start
                }
                oper_.append(row_for_oper)
        if row_ask['Дефицит'] == 0:
            break  # если потребность 0, то следующая строчка


def replacement(
    ind: int,
    sklad: DataFrame,
    table: DataFrame,
    oper_: list,
    nom_: DataFrame,
    repl_: dict
) -> None:
    """Списание со склада взаимозаменяемой номенклатуры

    :param ind: индекс строчки в таблице потребностей
    :param sklad: склад списания
    :param table: таблица потребностей
    :param oper_: писок операций
    :param nom_: справочник номенклатуры
    :param repl_: словарь со справочниками замен {
            'mark': dict_repl_mark,
            'gost': dict_repl_gost,
            'pokr': dict_repl_pokr,
            'prochn': dict_repl_prochn
        }
    """
    if table.at[ind, 'Дефицит'] == 0 or table.at[ind, 'Нельзя_заменять'] == 1:
        return None  # быстрый выход из списания, если потребность = 0 или нельзя менять

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

    for i in need_replacements.index:  # i - это индекс найденных строчек в остатках по датам

        row_ask = table.loc[ind].copy()
        ask_start = row_ask['Дефицит']
        row_rest = sklad.loc[i].copy()
        rest_nom_start = row_rest['Количество']

        if rest_nom_start == 0:
            pass
        else:
            if 0 > (rest_nom_start - ask_start):  # если не полность покрывается остатком
                row_rest['Количество'] = 0
                sklad.loc[i] = row_rest
                row_ask['Дефицит'] = ask_start - rest_nom_start
                table.loc[ind] = row_ask

                row_for_oper = {
                    'Дата потребности': row_ask['Дата запуска'],
                    'Порядковый номер': row_ask['Поряд_номер'],
                    'Заказ-Партия': row_ask['Заказ-Партия'],
                    'Номенклатура потребности': cur_nom,
                    'Потребность из файла': row_ask['Дефицит'],
                    'Потребность нач': ask_start,
                    'Потребность кон': ask_start - rest_nom_start,
                    'Списание потребности': rest_nom_start,
                    'Склад': row_rest['Склад'],
                    'Дата списания остат': row_rest['Дата'],
                    'Номенклатура Списания': row_rest['Номенклатура'],
                    'Остатки нач': rest_nom_start,
                    'Остатки кон': 0,
                    'Списание остатков': rest_nom_start
                }
                oper_.append(row_for_oper)
            else:
                row_rest['Количество'] = rest_nom_start - ask_start
                sklad.loc[i] = row_rest
                row_ask['Дефицит'] = 0
                table.loc[ind] = row_ask

                row_for_oper = {
                    'Дата потребности': row_ask['Дата запуска'],
                    'Порядковый номер': row_ask['Поряд_номер'],
                    'Заказ-Партия': row_ask['Заказ-Партия'],
                    'Номенклатура потребности': cur_nom,
                    'Потребность из файла': row_ask['Дефицит'],
                    'Потребность нач': ask_start,
                    'Потребность кон': 0,
                    'Списание потребности': ask_start,
                    'Склад': row_rest['Склад'],
                    'Дата списания остат': row_rest['Дата'],
                    'Номенклатура Списания': row_rest['Номенклатура'],
                    'Остатки нач': rest_nom_start,
                    'Остатки кон': rest_nom_start - ask_start,
                    'Списание остатков': ask_start
                }
                oper_.append(row_for_oper)

        if row_ask['Дефицит'] == 0:
            break  # если потребность 0, то следующая строчка


def search_replacements(
    cur_nom: str,
    sklad: DataFrame,
    dict_nom: DataFrame,
    dict_repl: dict
) -> DataFrame:
    """Поиск взаимозамен по нескольким параметрам из словарь со справочниками замен

    :param cur_nom: текущая номенклатура
    :param sklad: склад списаний
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
        [sklad['Количество'] > 0]\
        [sklad['Номенклатура'].str.startswith(cur_nom.split()[0])].\
        copy()

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
    if cur_mark != '':
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
