"""Analysis of shipments
Анализ закупок менеджеров по отношению к недельному отчету расчету закупа"""

from etl.extract import (
    replacements, load_processed_deficit,
    load_orders_to_supplier, nomenclature
)
from etl.extract import (
    PATH_REP_MARK, PATH_REP_GOST,
    PATH_REP_POKR, PATH_REP_PROCHN
)
from algo.search import building_purchase_analysis


def main() -> None:
    """Главная функция анализа закупок"""
    processed_deficit = load_processed_deficit()
    orders = load_orders_to_supplier()

    dict_nom = nomenclature()
    dict_repl_mark = replacements(PATH_REP_MARK)
    dict_repl_gost = replacements(PATH_REP_GOST)
    dict_repl_pokr = replacements(PATH_REP_POKR)
    dict_repl_prochn = replacements(PATH_REP_PROCHN)

    building_purchase_analysis(
        table=processed_deficit,
        orders=orders,
        nom_=dict_nom,
        repl_={
            'mark': dict_repl_mark,
            'gost': dict_repl_gost,
            'pokr': dict_repl_pokr,
            'prochn': dict_repl_prochn
        }
    )


if __name__ == '__main__':
    main()
