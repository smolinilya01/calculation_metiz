"""Execute excel macros"""

import logging
import win32com.client

from etl.extract import NOW
from os import path
from shutil import copy
from traceback import format_exc


def weekly_excel_reports() -> None:
    """Формирование еженедельных отчетов в эксель файлах"""
    weekly_report_name = r".\support_data\reports\Итоговая_потребность.xlsm"
    macro(weekly_report_name)


def daily_excel_reports() -> None:
    """Формирование ежедневных отчетов в эксель файлах"""
    daily_report_name = r".\support_data\reports\Дефицит.xlsm"
    macro(daily_report_name)
    destination = (
        r'\\oemz-fs01.oemz.ru\Works$\1.1. Отчеты по производству\1.1.5 Отчет по дефициту заказов' +
        f'\\{NOW.strftime("%Y%m%d")}_Дефицит.xlsm'
    )
    try:
        copy(daily_report_name, destination)
    except Exception:
        copy(daily_report_name.replace('.xlsm', '_1.xlsm'), destination)
        logging.info(format_exc)


def macro(path_: str) -> None:
    """Запуск макроса в excel файле.
    !!!!!!!Макрос всегда в модуле 1 и называется load_data!!!!!!!!

    :param path_: относительнвый путь к excel файлу с макросом
    """
    abs_path = path.abspath(path_)
    name_file = path.basename(abs_path)
    if path.exists(abs_path):
        excel_macro = win32com.client.DispatchEx("Excel.Application")  # DispatchEx is required in the newest versions of Python.
        excel_path = path.expanduser(abs_path)
        workbook = excel_macro.Workbooks.Open(Filename=excel_path, ReadOnly=1)
        excel_macro.Application.Run(f"{name_file}!Module1.load_data")
        workbook.Save()
        excel_macro.Application.Quit()
        del excel_macro
