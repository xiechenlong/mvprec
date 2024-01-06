import os
import numpy as np
import pandas as pd
from glob import glob
import subprocess
import time


class RankDataHandler:
    def __init__(self, table_name, data_directory='/data'):
        self.table_name = table_name
        self.finish_table_name = f"{table_name}_finish"
        self.table_directory = os.path.join(data_directory, self.table_name)
        self.field_delimiter = '|'

        # 确保目录存在
        os.makedirs(self.table_directory, exist_ok=True)
        
    def _get_partitioned_directory(self, partition_spec):
        return os.path.join(self.table_directory, partition_spec.replace('=', '_'))
        
    def download_data(self, partition_spec, threads=2):
        partitioned_directory = self._get_partitioned_directory(partition_spec)
        os.makedirs(partitioned_directory, exist_ok=True)
        
        # 下载标记数据更新完成的表，使用单线程
        download_data_from_odps(self.finish_table_name, partition_spec, 
                                os.path.join(partitioned_directory, "finish.csv"),
                                self.field_delimiter, header=True, threads=1)

        # 下载实际的数据表
        download_data_from_odps(self.table_name, partition_spec, 
                                self.table_directory,
                                self.field_delimiter, header=False, threads=threads)

        # 下载表头
        download_data_from_odps(self.table_name, partition_spec, 
                                os.path.join(partitioned_directory, "header.csv"),
                                self.field_delimiter, header=True, limit=1, threads=1)

    def read_data(self, partition_specs, model_inputs, labels, batch_size):
        """
        从给定分区读取数据，并根据模型输入信息处理特征数据，同时处理给定的标签数据，生成按批次准备好的特征和标签。

        Args:
            partition_specs (List[str]): 分区规格列表，用来指定哪些分区的数据需要被下载和读取。
            model_inputs (List[dict]): 模型输入信息列表，每个字典包含一个输入层的名称(name)、形状(shape)和数据类型(dtype)。
            labels (List[str]): 标签列名称列表。
            batch_size (int): 指定每个批次的样本数量。

        Yields:
            generator of (features, label_batch) (tuple): 对于每个批次，返回一个元组(features, label_batch)。
            `features` 是一个字典，包含处理后的特征数据，键是特征名称，值是对应的NumPy数组。
            `label_batch` 是一个字典，包含处理后的标签数据，键是标签名称，值是对应的NumPy数组。

        Example:
            # 创建数据下载器实例
            data_downloader = DataDownloader('my_table', '/path/to/odpscmd', '/path/to/config.ini')
            
            # 定义模型输入特征信息
            model_inputs = [
                {'name': 'feature1', 'shape': (50,), 'dtype': 'int32'},
                {'name': 'feature2', 'shape': (1,), 'dtype': 'int32'},
                # ...
            ]
            
            # 定义标签列名
            labels = ['label1', 'label2']
            
            # 定义分区规格列表和批量大小
            partition_specs = ['pt=20220101', 'pt=20220102']
            batch_size = 128
            
            # 使用生成器读取数据
            data_iterator = data_downloader.read_data(partition_specs, model_inputs, labels, batch_size)
            
            # 遍历生成器获取批次数据
            for feature_batch, label_batch in data_iterator:
                # 在这里处理每个批次的数据
                # feature_batch 是一个字典，键是特征的名称，值是对应的NumPy数组
                # label_batch 是一个字典，键是标签的名称，值是对应的NumPy数组
                ...
        """
        for partition_spec in partition_specs:
            partitioned_directory = self._get_partitioned_directory(partition_spec)
           
            # Read the header for column names
            header_file = os.path.join(partitioned_directory, "header.csv")
            with open(header_file, 'r') as f:
                header = f.readline().strip().split(self.field_delimiter)
            
            data_files = glob(os.path.join(partitioned_directory, f"{partition_spec}_*"))
            for data_file in data_files:
                for chunk in pd.read_csv(data_file, delimiter=self.field_delimiter, names=header, chunksize=batch_size):
                    feature_batch = {}
                    label_batch = {}
                    # Process features
                    for feature in model_inputs:
                        feature_name = feature['name']
                        if feature['shape'][0] > 1:
                            feature_batch[feature_name] = np.array(chunk[feature_name].str.split(',').tolist(), dtype=feature['dtype'])
                        else:
                            feature_batch[feature_name] = chunk[feature_name].astype(feature['dtype']).values
                    
                    # Process labels
                    for label in labels:
                        label_data = chunk[label].astype('float32' if 'float' in chunk[label].dtype.name else 'int32')
                        label_batch[label] = label_data.values
                    
                    yield feature_batch, label_batch


def check_partition_exists(table_name, partition_spec, odpscmd_path='/home/odpscmd'):
    """
    检查ODPS表的分区是否存在

    :param table_name: ODPS表名
    :param partition_spec: 分区规格字符串
    :param odpscmd_path: ODPS命令行工具的路径，默认为 'odpscmd'
    :return: 存在返回True，不存在返回False
    """
    command = f'{odpscmd_path} -e "desc {table_name} partition({partition_spec})"'
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # 如果描述分区的命令执行成功，说明分区存在
    return result.returncode == 0

def download_data_from_odps(table_name, partition_spec, download_file, odpscmd_path='/home/odpscmd',
                            field_delimiter=',', header=False, limit=None, threads=1):
    """
    使用ODPS命令行工具下载数据到本地文件，并支持指定字段分隔符、是否包含表头、限制下载记录数和下载使用的线程数。

    :param table_name: ODPS表名
    :param partition_spec: 分区规格字符串，例如 'pt=20220101'
    :param download_file: 下载到本地的文件路径
    :param odpscmd_path: ODPS命令行工具的路径
    :param field_delimiter: 字段分隔符，默认为逗号
    :param header: 是否在文件中包含表头，默认为False
    :param limit: 限制下载的记录数，默认为None，即下载所有记录
    :param threads: 下载时使用的线程数，默认为1
    :return: None
    """
    
    # 等待分区变得可用
    print(f"检查表 {table_name} 分区 {partition_spec} 是否存在...")
    while not check_partition_exists(table_name, partition_spec, odpscmd_path):
        print("分区不存在，等待...")
        time.sleep(60)  # 每分钟检查一次
    
    print("分区存在，开始下载数据...")
    
    # 将参数转换为命令行选项
    header_option = '-h' if header else ''
    limit_option = f'-limit {limit}' if limit is not None else ''
    field_delimiter_option = f'-fd {field_delimiter}'
    threads_option = f'-t {threads}'

    # 构建下载命令
    command = f'{odpscmd_path} -e "tunnel download {table_name} {partition_spec} {download_file} {header_option} {limit_option} {field_delimiter_option} {threads_option} -cp"'

    # 调用子进程执行命令
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("下载完成。")
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print("下载失败。")
        print(e.stdout.decode())
        print(e.stderr.decode())