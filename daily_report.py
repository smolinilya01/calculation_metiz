"""Create daily report
Для запуска ежедневного отчета по дефициту"""

import logging

from os import chdir
from algo.write_off import write_off
from etl.extract import (
    replacements, center_rests,
    tn_rests, future_inputs, requirements,
    nomenclature
)
from etl.extract import (
    PATH_REP_MARK, PATH_REP_GOST,
    PATH_REP_POKR, PATH_REP_PROCHN
)
from common.common import check_calculation_right
from reports.weekly import weekly_tables
from reports.daily import daily_tables
from reports.excel import daily_excel_reports
from traceback import format_exc


def main() -> None:
    """Выполнение расчетов"""
    operations = list()
    dict_nom = nomenclature()
    dict_repl_mark = replacements(PATH_REP_MARK)
    dict_repl_gost = replacements(PATH_REP_GOST)
    dict_repl_pokr = replacements(PATH_REP_POKR)
    dict_repl_prochn = replacements(PATH_REP_PROCHN)
    start_rest_center = center_rests(dictionary=dict_nom, short_term_plan=True)
    start_rest_tn = tn_rests(dictionary=dict_nom, short_term_plan=True)
    start_fut = future_inputs(dictionary=dict_nom, short_term_plan=True)
    start_ask = requirements(short_term_plan=True)
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
        sep_date=None
    )  # основные расчетные таблицы такие же, как и в недельном очтете
    daily_tables()
    daily_excel_reports()


if __name__ == '__main__':
    # chdir(r'C:\LOG_1\calculation_metal')  # что бы планировщик заданий переключался на правильную директорию
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=r'.\logs.txt'
    )
    # logging.disable(level=logging.CRITICAL)
    try:
        main()
    except Exception:
        logging.info(format_exc())
