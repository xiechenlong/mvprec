import pandas as pd
import numpy as np

# 定义函数来检查当前桶是否符合阈值条件
def is_threshold_met(cumulative_proportion, cumulative_positives, min_sample_proportion, min_positive_samples):
    return cumulative_proportion >= min_sample_proportion and cumulative_positives >= min_positive_samples

# 定义函数来计算阈值
def calculate_thresholds(df, features, total_samples, total_clicks, min_sample_proportion, min_positive_samples):
    thresholds = {feature: [0] for feature in features}  # 初始化阈值列表，包含起始阈值 0

    for feature in features:
        feature_df = df.loc[df[feature] > 0].groupby(feature).agg(
            sample_proportion=pd.NamedAgg(column='sample_cnt', aggfunc=lambda x: x.sum() / total_samples),
            positive_samples=pd.NamedAgg(column='click_sample_cnt', aggfunc='sum')
        ).reset_index().sort_values(by=feature)

        cumulative_proportion = 0
        cumulative_positives = 0

        for _, row in feature_df.iterrows():
            cumulative_proportion += row['sample_proportion']
            cumulative_positives += row['positive_samples']

            if is_threshold_met(cumulative_proportion, cumulative_positives, min_sample_proportion, min_positive_samples):
                thresholds[feature].append(round(row[feature], 6))
                cumulative_proportion = 0  # 重置累积值
                cumulative_positives = 0

    return thresholds

# 主函数
def main():
    # 读取数据
    # df = pd.read_csv('/data/conversion_rate_data')

    # 生成测试数据
    np.random.seed(0)  # 设置随机种子以保证结果可重复
    num_records = 1000
    test_data = {
        'click_rate_1d': np.random.rand(num_records),
        'item_click_rate_1d': np.random.rand(num_records),
        'sample_cnt': np.random.randint(1, 1000, num_records),
        'click_sample_cnt': np.random.randint(0, 500, num_records)
    }
    df = pd.DataFrame(test_data)

    # 特征域列表
    features_to_process = ['click_rate_1d', 'item_click_rate_1d']

    # 计算样本总数和正样本总数
    total_samples = df['sample_cnt'].sum()
    total_clicks = df['click_sample_cnt'].sum()

    # 定义桶聚合条件
    min_sample_proportion = 0.005  # 最小样本占比
    min_positive_samples = 100     # 最小正样本数量

    # 计算阈值
    thresholds = calculate_thresholds(df, features_to_process, total_samples, total_clicks, min_sample_proportion, min_positive_samples)

    # 输出结果
    print("阈值列表:")
    for feature, thres in thresholds.items():
        print(f"{feature}(数量: {len(thres)}): {thres}")

if __name__ == "__main__":
    main()