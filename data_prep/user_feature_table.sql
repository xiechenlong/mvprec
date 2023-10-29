CREATE TABLE IF NOT EXISTS `user_feature_table`
(
    user_id STRING COMMENT "用户唯一ID"
    ,device_id STRING COMMENT "用户设备ID"
    ,country bigint COMMENT "国家编码"
    ,city bigint COMMENT "城市编码"
    ,register_time bigint COMMENT "注册时间编码"
    ,last_login_time bigint COMMENT "上次登录时间编码"
    ,device_model bigint COMMENT "设备型号编码"
    ,device_language bigint comment "设备语言编码"
    ,os bigint comment "操作系统编码"
    ,media_source bigint COMMENT "用户来源编码"
    ,campaign bigint COMMENT "投放广告活动编码"
    ,tags STRING COMMENT "用户tags"
    ,user_level bigint COMMENT "用户等级"
    ,active_1d bigint comment "近一天访问天数"
    ,active_30d bigint comment "近三十天访问天数"
    ,expose_cnt_1d bigint comment "近一天曝光次数编码"
    ,click_cnt_1d bigint COMMENT "近一天点击次数编码"
    ,cart_cnt_15d bigint COMMENT "近十五天加购次数编码"
    ,order_cnt_30d bigint COMMENT "近三十天下单次数编码"
    ,click_rate_1d bigint COMMENT "近一天点击率编码"
)
PARTITIONED BY
(
    ds STRING
)
LIFECYCLE 365
;

INSERT OVERWRITE TABLE user_feature_table PARTITION (ds = ${bdp.system.bizdate})
SELECT 
    user_id,
    device_id,
    NVL(country_encode, '0') as country,
    NVL(city_encode, '0') as city,
    CASE 
        WHEN register_day <= 1 THEN 0
        WHEN register_day <= 7 THEN 1
        WHEN register_day <= 90 THEN 2
        ELSE 3
    END AS register_time,
    CASE 
        WHEN last_login_day <= 1 THEN 0
        WHEN last_login_day <= 7 THEN 1
        WHEN last_login_day <= 90 THEN 2
        ELSE 3
    END AS last_login_time,
    NVL(device_model_encode, '0') as device_model,
    NVL(device_language_encode, '0') as device_language,
    NVL(os_encode, '0') as os,
    NVL(media_source_encode, '0') as media_source,
    NVL(campaign_encode, '0') as campaign,
    tags,
    user_level,
    active_1d,
    active_30d,
    LogBucket(expose_cnt_1d, 1.3, 50, 100) AS expose_cnt_1d,
    TruncateBucket(click_cnt_1d, 10) AS click_cnt_1d,
    TruncateBucket(cart_cnt_15d, 10) AS cart_cnt_15d,
    TruncateBucket(order_cnt_30d, 10) AS order_cnt_30d,
    ConversionRateBucket(click_cnt_1d, expose_cnt_1d, ctr_thresholds) AS click_rate_1d
FROM (
    SELECT 
        user_id, 
        device_id,
        datediff(to_date(${bdp.system.bizdate},'yyyymmdd'), from_unixtime(register_time)) as register_day,
        datediff(to_date(${bdp.system.bizdate},'yyyymmdd'), from_unixtime(last_login_time)) as last_login_day,
        tags,
        user_level,
        active_1d,
        active_30d,
        expose_cnt_1d,
        click_cnt_1d,
        cart_cnt_15d,
        order_cnt_30d,
        c.encode as country_encode,
        ci.encode as city_encode,
        dm.encode as device_model_encode,
        dl.encode as device_language_encode,
        o.encode as os_encode,
        ms.encode as media_source_encode,
        ca.encode as campaign_encode
    FROM user_table
    LEFT JOIN label_encoding_table c ON country = c.value AND c.feature = 'country' AND c.ds = ${bdp.system.bizdate}
    LEFT JOIN label_encoding_table ci ON city = ci.value AND ci.feature = 'city' AND ci.ds = ${bdp.system.bizdate}
    LEFT JOIN label_encoding_table dm ON device_model = dm.value AND dm.feature = 'device_model' AND dm.ds = ${bdp.system.bizdate}
    LEFT JOIN label_encoding_table dl ON device_language = dl.value AND dl.feature = 'device_language' AND dl.ds = ${bdp.system.bizdate}
    LEFT JOIN label_encoding_table o ON os = o.value AND o.feature = 'os' AND o.ds = ${bdp.system.bizdate}
    LEFT JOIN label_encoding_table ms ON media_source = ms.value AND ms.feature = 'media_source' AND ms.ds = ${bdp.system.bizdate}
    LEFT JOIN label_encoding_table ca ON campaign = ca.value AND ca.feature = 'campaign' AND ca.ds = ${bdp.system.bizdate}
    WHERE ds=${bdp.system.bizdate}
) t
LEFT JOIN (
    SELECT ctr_thresholds
) t1
ON 1 = 1
;