#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from csv import reader
from datetime import datetime
from logging import getLogger, ERROR, Formatter
from logging.handlers import RotatingFileHandler
from os import makedirs, walk, remove, system, chdir
from os.path import join, isdir, splitext, basename, exists, dirname, abspath
from shutil import get_terminal_size
from threading import Thread, Semaphore, Lock
from time import time
from typing import Union

from xlwt import Workbook, Alignment, Borders, XFStyle, Font


# 初始化类
class Initialization:
    # 初始化
    def __init__(self, input_path) -> None:
        self.paths = self.check_paths(input_path)  # 目标文件
        self.output_path = self.set_output_path(input_path)  # 成果输出目录
        makedirs(self.output_path, exist_ok=True)  # 判断目录不存在则创建
        self.logger = getLogger(__name__)  # log对象
        log_format = '%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s'
        formatter = Formatter(log_format)
        handler = RotatingFileHandler(join(self.output_path, 'error.log'), mode='a', maxBytes=1024 * 1024,
                                      backupCount=5,
                                      encoding='utf-8')
        handler.setFormatter(formatter)
        handler.setLevel(ERROR)
        self.logger.addHandler(handler)
        self.logger.setLevel(ERROR)

    # 错误日志记录
    def log(self, message) -> None:
        self.logger.exception(message)  # 记录错误日志

    # 判断源文件路径是否存在(必须是文件夹)
    def check_paths(self, input_path) -> Union[str, None]:
        if isdir(input_path):
            return input_path
        else:
            self.log('源文件路径不存在！无法读取数据！')
            self.logger.handlers.clear()
            exit()

    # 设置输出路径
    def set_output_path(self, input_path) -> Union[str, None]:
        if self.paths:
            return join(input_path, '原始数据提取成果')
        else:
            self.log('源文件路径不存在！无法设置输出路径！')
            self.logger.handlers.clear()
            exit()


class DataPreprocessing(Initialization):
    def __init__(self, input_file):
        # 调用父类的初始化方法
        super().__init__(input_file)
        # 获取文件列表
        self.files = self.get_files()

    # 获取文件列表
    def get_files(self) -> dict:
        """
        获取数据文件列表。

        遍历指定目录及其子目录，查找所有扩展名为 .TPT、.TZT 和 .TXT 的文件。
        将找到的文件按照目录名称分类，存储在字典中。

        :return: 一个字典，包含找到的文件。
        """
        result = {}
        try:
            for dir_path, _, file_names in walk(self.paths):
                folder_name = basename(dir_path)
                for file_name in file_names:
                    file_path = join(dir_path, file_name)
                    base, ext = splitext(file_name)
                    if ext.upper() in ['.TPT', '.TZT', '.TXT']:
                        result.setdefault(folder_name, {})[ext[1:].upper()] = file_path
            return result
        except OSError as err:
            self.log(err)
            self.logger.handlers.clear()
            exit()

    # 提取角度数据
    def processing_angle(self, file) -> list:
        """
        处理角度数据文件。

        读取指定的角度数据文件，提取其中的数据。
        将提取到的数据存储在列表中，并返回。

        :param file: 角度数据文件。
        :return: 一个列表，包含提取到的数据。
        """
        try:
            data = [row for row in reader(file)]
            station_info = [data[0][0], data[0][-1]]  # 测站,仪高
            returns = int(data[0][1])  # 总测回
            rows = int(data[0][2]) + 1  # 每测回数据行数
            date_info = [data[1][1].split('=')[1], data[1][2].split('=')[1]]
            data = data[2:-1]
            # 获取每测回数所在行的索引
            indices = [i for i, x in enumerate(data) if len(x) == 1]
            data = [x for i, x in enumerate(data) if i not in indices]
            limit = len(data)
            # 将每测回数插入到每测回数据前面
            for i in range(returns):
                for j in range(i * rows, (i + 1) * rows):
                    if j < limit:
                        data[j] = station_info + [str(i + 1)] + data[j] + date_info
                    else:
                        break
            return data
        except OSError as err:
            self.log(err)
            self.logger.handlers.clear()
            exit()

    # 提取距离数据
    def processing_distance(self, file) -> list:
        """
        处理距离数据文件。

        读取指定的距离数据文件，提取其中的数据。
        将提取到的数据存储在列表中，并返回。

        :param file: 距离数据文件。
        :return: 一个列表，包含提取到的数据。
        """
        results = []
        try:
            data = list(reader(file))
            station_info = [data[0][0], data[0][-1]]  # 测站,仪高
            date_info = [data[1][1].split('=')[1], data[1][3].split('=')[1]]
            data = data[2:-1]
            data = [sublist for sublist in data if "Dist Start" not in sublist and "Dist End" not in sublist]
            for row in data:
                results.append(station_info + row + date_info)
            return results

        except OSError as err:
            self.log(err)
            self.logger.handlers.clear()
            exit()

    # 合并数据
    @staticmethod
    def merge_data(tpt_data: list, tzt_data: list, txt_data: list) -> list:
        """
        合并三个列表中的数据。

        将三个列表中的数据严格匹配 [测站, 仪高, 测回, 目标点名]，然后进行数据合并。
        合并后的数据格式为 [测站, 仪高, 测回, 目标点名, 目标高, 水平角盘左,
        水平角盘右, 天顶距盘左, 天顶距盘右, 斜距, 日期, 时间]。

        :param tpt_data: TPT 数据列表。
        :param tzt_data: TZT 数据列表。
        :param txt_data: TXT 数据列表。
        :return: 一个列表，包含合并后的数据。
        """
        result = []
        for tpt_row in tpt_data:
            station_info = tpt_row[:4]
            tzt_row = next((row for row in tzt_data if row[:4] == station_info), [None] * len(tzt_data[0]))
            txt_row = next((row for row in txt_data if row[:4] == station_info), [None] * len(txt_data[0]))
            merged = station_info + [tzt_row[6]] + tpt_row[4:6] + tzt_row[4:6] + [txt_row[4]] + tpt_row[6:]
            result.append(merged)
        return result

    # 单线程处理数据
    def process_data(self, tpt_file, tzt_file, txt_file) -> list:
        """
        处理所有数据文件。

        读取指定的 TPT、TZT 和 TXT 数据文件，提取其中的数据。
        将提取到的数据进行合并，并输出结果。

        :param tpt_file: TPT 数据文件。
        :param tzt_file: TZT 数据文件。
        :param txt_file: TXT 数据文件。
        """
        try:
            with open(tpt_file, 'r', encoding='utf-8') as tpt_f, open(tzt_file, 'r', encoding='utf-8') as tzt_f, open(
                    txt_file, 'r', encoding='utf-8') as txt_f:
                tpt_data = self.processing_angle(tpt_f)
                tzt_data = self.processing_angle(tzt_f)
                txt_data = self.processing_distance(txt_f)
                data = self.merge_data(tpt_data, tzt_data, txt_data)
                return data
        except OSError as err:
            self.log(err)
            self.logger.handlers.clear()
            exit()

    # 输出Excel表格
    def output_excel(self, data, file_name) -> None:
        try:
            # 将三维列表转换为二维列表
            data = [row for sublist in data for row in sublist]

            wb = Workbook('encoding = utf-8')
            sht = wb.add_sheet('sheet1', cell_overwrite_ok=True)

            # 设置单元格样式
            alignment = Alignment()
            alignment.horz = Alignment.HORZ_CENTER
            alignment.vert = Alignment.VERT_CENTER
            borders = Borders()
            borders.left = Borders.THIN
            borders.right = Borders.THIN
            borders.top = Borders.THIN
            borders.bottom = Borders.THIN
            style = XFStyle()
            style.alignment = alignment
            style.borders = borders

            # 设置表头样式
            font = Font()
            font.bold = True
            header_style = XFStyle()
            header_style.font = font
            header_style.alignment = alignment
            header_style.borders = borders

            headers = ['测站', '仪高', '测回', '目标点名', '目标高', '水平角', '', '天顶距', '', '斜距', '日期', '时间']
            sub_headers = ['', '', '', '', '', '盘左', '盘右', '盘左', '盘右', '', '']
            merge_info = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 6), (7, 8), (9, 9), (10, 10), (11, 11)]

            for col, header in enumerate(headers):
                sht.write(0, col, header, header_style)

            for col, sub_header in enumerate(sub_headers):
                sht.write(1, col, sub_header, header_style)

            for start_col, end_col in merge_info:
                if start_col == end_col:
                    sht.write_merge(0, 1, start_col, end_col, headers[start_col], header_style)
                else:
                    sht.write_merge(0, 0, start_col, end_col, headers[start_col], header_style)

            row_offset = 2
            for row in range(len(data)):
                for col in range(len(data[row])):
                    sht.write(row + row_offset, col, data[row][col], style)
                    # 根据内容自适应列宽
                    current_width = sht.col(col).width
                    new_width = int(max(current_width / 256.0 - 1.0, len(str(data[row][col])) + 2.0) * 256.0)
                    if new_width > current_width:
                        sht.col(col).width = new_width

            save_file = join(self.output_path, file_name)
            # 检查文件如果存在则覆盖
            if exists(save_file):
                remove(save_file)
            wb.save(save_file)
        except OSError as err:
            self.log(err)
            self.logger.handlers.clear()
            exit()

    # 命令栏进度条
    @staticmethod
    def progress_bar(progress: float, status: str = '',
                     bar_len: int = int(get_terminal_size().columns * 0.8)) -> None:
        """
        在命令台显示进度条。

        :param progress: 进度百分比。
        :param status: 进度条中显示的文字。
        :param bar_len: 进度条长度。
        """
        block = int(round(bar_len * progress))
        text = '\r[{}{}] {:.2f}% {}'.format(
            '\033[31m' + '#' * block + '\033[0m',
            '\033[32m' + '-' * (bar_len - block) + '\033[0m',
            round(progress * 100, 2), status)
        print(text, end='')

    def process_threaded(self, max_threads=16) -> None:
        """
        多线程处理数据。

        遍历文件路径合集，获取对应的三个文件。
        使用多线程技术并行调用 process_data 方法处理这些文件。

        :param max_threads: 最大线程数。
        """
        # 开始计时
        start_time = time()
        data = []
        data_lock = Lock()
        threads = []
        semaphore = Semaphore(max_threads)
        total_files = len(self.files)
        processed_files = 0

        def process_files(tpt, tzt, txt):
            nonlocal processed_files
            semaphore.acquire()
            try:
                temp = self.process_data(tpt, tzt, txt)
                with data_lock:
                    data.append(temp)
                    processed_files += 1
                    # 提示用户处理进度
                    progress = processed_files / total_files
                    self.progress_bar(progress, '处理进度：{:.2f}%'.format(progress * 100))
            finally:
                semaphore.release()

        for folder_name, file_dict in self.files.items():
            tpt_file = file_dict.get('TPT')
            tzt_file = file_dict.get('TZT')
            txt_file = file_dict.get('TXT')
            if tpt_file and tzt_file and txt_file:
                thread = Thread(target=process_files, args=(tpt_file, tzt_file, txt_file))
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()

        # 对二维列表按照日期和时间进行排序
        data.sort(key=lambda x: datetime.strptime(x[0][10] + ' ' + x[0][11], '%Y-%m-%d %H:%M:%S'))

        # 输出到本地excel文件
        self.output_excel(data, '导出成果.xls')

        # 结束计时
        end_time = time()
        print('\n处理完成，耗时：{:.2f}秒'.format(end_time - start_time))


# Test Code
if __name__ == '__main__':
    # Leica多测回数据文件夹路径
    path = r'C:\Users\laozh\Desktop\新建文件夹'
    try:
        # 设置requirements.txt在当前目录下
        chdir(dirname(abspath(__file__)))
        system('pip install -r requirements.txt')
        # 检查路径不存在则创建
        if not exists(path):
            makedirs(path)
        # 实例化数据预处理
        data_processing = DataPreprocessing(path)
        # 多线程处理数据
        data_processing.process_threaded()
    except OSError as err:
        print(err)
        exit()
