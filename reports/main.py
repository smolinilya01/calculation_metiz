"""Building reports"""

from pandas import DataFrame
from reports.weekly import weekly_tables
from reports.excel import weekly_excel_reports
from datetime import datetime


def weekly_reports(
    start_ask_: DataFrame,
    end_ask_: DataFrame,
    oper_: list,
    sep_date: datetime
) -> None:
    """Построение всех отчетов

    :param start_ask_: Таблица потребностей до списаний
    :param end_ask_: Таблица потребностей после списаний
    :param oper_: Таблица с операциями списания
    :param sep_date: Дата разделения краткосрочного периода и долгосрочного = последний день краткосрочного
    """
    weekly_tables(
        start_ask_=start_ask_,
        end_ask_=end_ask_,
        oper_=oper_,
        sep_date=sep_date
    )
    weekly_excel_reports()
