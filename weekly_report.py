"""Create weekly report"""

import logging

from algo.write_off import write_off
from etl.extract import (
    replacements, center_rests,
    tn_rests, future_inputs,
    nomenclature, requirements
)
from etl.extract import (
    PATH_REP_MARK, PATH_REP_GOST,
    PATH_REP_POKR, PATH_REP_PROCHN
)
from common.common import check_calculation_right
from reports.weekly import weekly_tables
from reports.excel import weekly_excel_reports
from datetime import datetime, timedelta


def main(shift_days: int = None) -> None:
    """Выполнение расчетов"""
    # shift_days используется для расчета в анализе закупа, если установить shift_days,
    # то посчитает потребность на shift_days дней вперед
    if shift_days is None:
        separate_date = input('Введите дату окончания краскосрочного периода,\nВ формате ДД.ММ.ГГГГ:\n')
        separate_date = datetime.strptime(separate_date, '%d.%m.%Y')
    else:
        separate_date = datetime.now() + timedelta(days=shift_days)

    operations = list()
    dict_nom = nomenclature()
    dict_repl_mark = replacements(PATH_REP_MARK)
    dict_repl_gost = replacements(PATH_REP_GOST)
    dict_repl_pokr = replacements(PATH_REP_POKR)
    dict_repl_prochn = replacements(PATH_REP_PROCHN)
    start_rest_center = center_rests(dictionary=dict_nom)
    start_rest_tn = tn_rests(dictionary=dict_nom)
    start_fut = future_inputs(dictionary=dict_nom)
    start_ask = requirements()
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
        sep_date=separate_date
    )
    weekly_excel_reports()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # logging.disable(level=logging.CRITICAL)
    main()
