CREATE TABLE conversion_rate_data AS
SELECT
--注意其中的 click_rate_1d、item_click_rate_1d 需要先做平滑处理，目的是和 ConversionRateBucket 这个 udf 函数的逻辑保持一致。
    COALESCE(user_feature_day_before.click_rate_1d, 0) AS click_rate_1d, 
    COALESCE(item_feature_day_before.item_click_rate_1d, 0) AS item_click_rate_1d,
    COUNT(*) AS sample_cnt,
    SUM(rank_sample.is_click) AS click_sample_cnt
FROM
    rank_sample_table rank_sample
LEFT JOIN 
    user_feature_table user_feature_day_before
ON 
    rank_sample.user_id = user_feature_day_before.user_id 
    AND user_feature_day_before.ds = TO_CHAR(DATE_SUB(TO_DATE('${bdp.system.bizdate}', 'yyyymmdd'), 1), 'yyyymmdd')
LEFT JOIN 
    item_feature_table item_feature_day_before
ON 
    rank_sample.item_id = item_feature_day_before.item_id 
    AND item_feature_day_before.ds = TO_CHAR(DATE_SUB(TO_DATE('${bdp.system.bizdate}', 'yyyymmdd'), 1), 'yyyymmdd')
WHERE 
    rank_sample.ds = '${bdp.system.bizdate}'
GROUP BY
    COALESCE(user_feature_day_before.click_rate_1d, 0),
    COALESCE(item_feature_day_before.item_click_rate_1d, 0);