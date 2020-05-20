"""Analysis of shipments
Анализ закупок менеджеров по отношению к недельному отчету расчету закупа"""

import logging

from etl.extract import PATH_FOR_DATE
from algo.search import building_purchase_analysis
from pandas import read_excel, DataFrame
from datetime import datetime, timedelta
from os import path as os_path
from weekly_report import main as building_weekly_report
from reports.excel import purchase_analyze_reports


def main() -> None:
    """Главная функция анализа закупок"""
    table = building_purchase_analysis()

    # сбор сводной строчки строчки
    summary_columns = [
        'Дата плана закупа', 'Дата анализа', 'Расчетный план закупа',
        'Дефицит на дату плана закупа', 'Заказано', 'Поступило',
        'Остаточная поребность', 'Дефицит на дату анализа', 'Выполнение плана закупа'
    ]
    summary_row = DataFrame(
        data=None,
        columns=summary_columns
    )
    summary_row.loc[0, 'Дата плана закупа'] = datetime.\
        fromtimestamp(os_path.getmtime(PATH_FOR_DATE)).date()
    summary_row.loc[0, 'Дата анализа'] = datetime.now().date()
    summary_row.loc[0, 'Расчетный план закупа'] = table['План_закупа'].sum()
    summary_row.loc[0, 'Дефицит на дату плана закупа'] = cur_deficit_plan()
    summary_row.loc[0, 'Заказано'] = table['Заказано'].sum()
    summary_row.loc[0, 'Поступило'] = table['Доставлено'].sum()
    summary_row.loc[0, 'Остаточная поребность'] = table['Еще_заказать'].sum()
    summary_row.loc[0, 'Дефицит на дату анализа'] = cur_deficit_fact()
    summary_row.loc[0, 'Выполнение плана закупа'] = (
        (summary_row.loc[0, 'Расчетный план закупа'] - summary_row.loc[0, 'Остаточная поребность'])
        / summary_row.loc[0, 'Расчетный план закупа']
    )

    summary_row.to_excel(
        r".\support_data\purchase_analysis\summary_row.xlsx",
        index=False
    )

    purchase_analyze_reports()


def cur_deficit_plan() -> float:
    """Считает дефицит на 2 дня вперед в расчитанном плане закупа
    (на потребность списываются только складские остатки)"""
    path = r".\support_data\purchase_analysis\Итоговая_потребность.xlsm"
    date = datetime.fromtimestamp(os_path.getmtime(path))

    data = read_excel(
        path,
        sheet_name='Списания',
        usecols=[0, 4, 6, 9],
        parse_dates=['Дата запуска']
    )
    data = data[data['Дата запуска'] <= (date + timedelta(days=2))]
    data['Остаток дефицита'] += data['Списание из Поступлений']

    return data['Остаток дефицита'].sum()


def cur_deficit_fact() -> float:
    """Считает дефицит на 2 дня вперед по новым данным
    (на потребность списываются только складские остатки)"""
    building_weekly_report(shift_days=2)
    path = r".\support_data\reports\Итоговая_потребность.xlsm"

    data = read_excel(
        path,
        sheet_name='Списания',
        usecols=[0, 4, 6, 9],
        parse_dates=['Дата запуска']
    )
    data['Остаток дефицита'] += data['Списание из Поступлений']

    return data['Остаток дефицита'].sum()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
