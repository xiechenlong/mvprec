CREATE TABLE item_feature_table
(
    item_id STRING COMMENT "内容唯一标识ID",
    cate_id_1 bigint COMMENT "一级类目",
    cate_id_2 bigint COMMENT "二级类目",
    cate_id_3 bigint COMMENT "三级类目",
    shop_id STRING COMMENT "商店id",
    tags STRING COMMENT "标签",
    tags_len bigint COMMENT "标签非零元素个数",
    status STRING COMMENT "是否可推荐",
    publish_time bigint COMMENT "发布时间",
    price bigint COMMENT "原始价格",
    expose_cnt_1d bigint comment "近一天曝光次数编码",
    click_cnt_1d bigint COMMENT "近一天点击次数编码",
    cart_uv_15d bigint COMMENT "近十五天加购人数编码",
    order_uv_30d bigint COMMENT "近三十天下单人数编码",
    click_rate_1d bigint COMMENT "近一天点击率编码"
)
PARTITIONED BY 
(
    ds STRING
)
LIFECYCLE 365
;

INSERT OVERWRITE TABLE item_feature_table PARTITION (ds = ${bdp.system.bizdate})
SELECT 
    item_id,
    NVL(cate_id_1, 0) as cate_id_1,
    NVL(cate_id_2, 0) as cate_id_2,
    NVL(cate_id_3, 0) as cate_id_3,
    NVL(shop_id, 0) as shop_id,
    CONCAT_WS(',', SLICE(SPLIT(CONCAT_WS(',', NVL(tags, ''), REPEAT('0,', 100)), ','), 1, 100)) AS tags,
    size(SLICE(SPLIT(NVL(tags,''), ','), 1, 100)) as tags_len,
    status,
    CASE 
        WHEN publish_day <= 1 THEN 0
        WHEN publish_day <= 7 THEN 1
        WHEN publish_day <= 90 THEN 2
        ELSE 3
    END AS publish_time,
    CASE 
        WHEN price <= 1 THEN 0
        WHEN price <= 10 THEN 1
        WHEN price <= 100 THEN 2
        ELSE 3
    END AS price,
    LogBucket(expose_cnt_1d, 1.5, 50, 100) AS expose_cnt_1d,
    TruncateBucket(click_cnt_1d, 10) AS click_cnt_1d,
    TruncateBucket(cart_uv_15d, 10) AS cart_uv_15d,
    TruncateBucket(order_uv_30d, 10) AS order_uv_30d,
    ConversionRateBucket(click_cnt_1d, expose_cnt_1d, ctr_thresholds) AS click_rate_1d
FROM (
    SELECT 
        item_id, 
        cate_id_1,
        cate_id_2,
        cate_id_3,
        shop_id,
        tags,
        status,
        datediff(to_date(${bdp.system.bizdate},'yyyymmdd'), to_date(publish_time, 'yyyymmdd')) as publish_day,
        price,
        expose_cnt_1d,
        click_cnt_1d,
        cart_uv_15d,
        order_uv_30d
    FROM item_table 
    WHERE ds=${bdp.system.bizdate}
) t
LEFT JOIN (
    SELECT ctr_thresholds
) t1
ON 1 = 1
;