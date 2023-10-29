-- 创建一个新表来存储编码
CREATE TABLE IF NOT EXISTS `label_encoding_table`
(
    feature STRING COMMENT "特征名"
    ,value STRING COMMENT "特征值"
    ,encode BIGINT COMMENT "编码值"
)
PARTITIONED BY 
(
    ds STRING
)
LIFECYCLE 365
;

INSERT OVERWRITE TABLE label_encoding_table PARTITION (ds = ${bdp.system.bizdate})
SELECT feature, value, encode
FROM label_encoding_table
WHERE ds = to_char(dateadd(to_date(${bdp.system.bizdate}, 'yyyymmdd'), -1, 'dd'), 'yyyymmdd')
UNION ALL
SELECT t1.feature, t1.value, NVL(max_encode, 0) + ROW_NUMBER() OVER (PARTITION BY t1.feature ORDER BY t1.value) AS encode
FROM (
    SELECT 'country' as feature, country as value FROM user_table WHERE ds = ${bdp.system.bizdate} AND active_1d = 1 GROUP BY country HAVING COUNT(*) > 1000
    UNION ALL
    SELECT 'city' as feature, city as value FROM user_table WHERE ds = ${bdp.system.bizdate} AND active_1d = 1 GROUP BY city HAVING COUNT(*) > 1000
    UNION ALL
    SELECT 'device_model' as feature, device_model as value FROM user_table WHERE ds = ${bdp.system.bizdate} AND active_1d = 1 GROUP BY device_model HAVING COUNT(*) > 1000
    UNION ALL
    SELECT 'device_language' as feature, device_language as value FROM user_table WHERE ds = ${bdp.system.bizdate} AND active_1d = 1 GROUP BY device_language HAVING COUNT(*) > 1000
    UNION ALL
    SELECT 'os' as feature, os as value FROM user_table WHERE ds = ${bdp.system.bizdate} AND active_1d = 1 GROUP BY os HAVING COUNT(*) > 1000
    UNION ALL
    SELECT 'media_source' as feature, media_source as value FROM user_table WHERE ds = ${bdp.system.bizdate} AND active_1d = 1 GROUP BY media_source HAVING COUNT(*) > 1000
    UNION ALL
    SELECT 'campaign' as feature, campaign as value FROM user_table WHERE ds = ${bdp.system.bizdate} AND active_1d = 1 GROUP BY campaign HAVING COUNT(*) > 1000
) t1
LEFT ANTI JOIN label_encoding_table t2 ON t1.feature = t2.feature AND t1.value = t2.value AND t2.ds = to_char(dateadd(to_date(${bdp.system.bizdate}, 'yyyymmdd'), -1, 'dd'), 'yyyymmdd')
LEFT JOIN (
    SELECT feature, MAX(encode) as max_encode
    FROM label_encoding_table
    WHERE ds = to_char(dateadd(to_date(${bdp.system.bizdate}, 'yyyymmdd'), -1, 'dd'), 'yyyymmdd')
    GROUP BY feature
) t3 ON t1.feature = t3.feature
;