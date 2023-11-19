CREATE TABLE IF NOT EXISTS `rank_train_data_table`
(
    user_id STRING COMMENT "用户ID",
    item_id STRING COMMENT "内容ID",
    trace_id STRING COMMENT "内容来源详细埋点",
    bhv_time bigint COMMENT "行为发生的时间戳",
    pos bigint COMMENT "商品所在的位置信息",
    session_id STRING COMMENT "会话ID",
    is_click BIGINT COMMENT "是否点击",
    is_cart BIGINT COMMENT "是否加入购物车",
    country bigint COMMENT "国家编码",
    city bigint COMMENT "城市编码",
    register_time bigint COMMENT "注册时间编码",
    last_login_time bigint COMMENT "上次登录时间编码",
    device_model_code bigint COMMENT "设备型号编码",
    device_language bigint COMMENT "设备语言编码",
    os bigint COMMENT "操作系统编码",
    media_source bigint COMMENT "用户来源编码",
    campaign bigint COMMENT "投放广告活动编码",
    user_tags STRING COMMENT "用户tags",
    user_tags_len STRING COMMENT "用户标签非零元素个数",
    user_level bigint COMMENT "用户等级",
    active_1d bigint COMMENT "近一天访问天数",
    active_30d bigint COMMENT "近三十天访问天数",
    expose_cnt_1d bigint COMMENT "近一天曝光次数编码",
    click_cnt_1d bigint COMMENT "近一天点击次数编码",
    cart_cnt_15d bigint COMMENT "近十五天加购次数编码",
    order_cnt_30d bigint COMMENT "近三十天下单次数编码",
    click_rate_1d bigint COMMENT "近一天点击率编码",
    click_itemids STRING COMMENT "近30条点击的itemid",
    cart_itemids STRING COMMENT "近30条加购的itemid",
    click_len bigint COMMENT "近30条点击的itemid非零元素个数",
    cart_len bigint COMMENT "近30条加购的itemid非零元素个数"
    cate_id_1 bigint COMMENT "一级类目",
    cate_id_2 bigint COMMENT "二级类目",
    cate_id_3 bigint COMMENT "三级类目",
    shop_id STRING COMMENT "商店id",
    tags STRING COMMENT "标签",
    tags_len bigint COMMENT "标签非零元素个数",
    publish_time bigint COMMENT "发布时间",
    price bigint COMMENT "原始价格",
    item_expose_cnt_1d bigint COMMENT "近一天曝光次数编码",
    item_click_cnt_1d bigint COMMENT "近一天点击次数编码",
    item_cart_uv_15d bigint COMMENT "近十五天加购人数编码",
    item_order_uv_30d bigint COMMENT "近三十天下单人数编码",
    item_click_rate_1d bigint COMMENT "近一天点击率编码",
    country_item_id STRING COMMENT "交叉特征，国家_内容ID"
)
PARTITIONED BY
(
    ds STRING
)
LIFECYCLE 365;


INSERT OVERWRITE TABLE rank_train_data_table PARTITION (ds = '${bdp.system.bizdate}')
SELECT
    rank_sample.user_id,
    rank_sample.item_id,
    rank_sample.trace_id,
    rank_sample.bhv_time,
    rank_sample.pos,
    rank_sample.session_id,
    rank_sample.is_click,
    rank_sample.is_cart,
    user_feature.country,
    user_feature.city,
    user_feature.register_time,
    user_feature.last_login_time,
    user_feature.device_model,
    user_feature.device_language,
    user_feature.os,
    user_feature.media_source,
    user_feature.campaign,
    COALESCE(user_feature_day_before.user_tags, REPEAT('0,', 10)),
    COALESCE(user_feature_day_before.user_tags_len, 0),
    user_feature.user_level,
    COALESCE(user_feature_day_before.active_1d, 0),
    COALESCE(user_feature_day_before.active_30d, 0),
    COALESCE(user_feature_day_before.expose_cnt_1d, 0),
    COALESCE(user_feature_day_before.click_cnt_1d, 0),
    COALESCE(user_feature_day_before.cart_cnt_15d, 0),
    COALESCE(user_feature_day_before.order_cnt_30d, 0),
    COALESCE(user_feature_day_before.click_rate_1d, 0),
    BehaviorFilter(rank_sample.click_itemid_30d, rank_sample.bhv_time, 30, true) AS click_itemids,
    BehaviorFilter(rank_sample.cart_itemid_30d, rank_sample.bhv_time, 30, true) AS cart_itemids,
    BehaviorLen(rank_sample.click_itemid_30d, rank_sample.bhv_time, 30) AS click_len,
    BehaviorLen(rank_sample.cart_itemid_30d, rank_sample.bhv_time, 30) AS cart_len,
    item_feature.cate_id_1,
    item_feature.cate_id_2,
    item_feature.cate_id_3,
    item_feature.shop_id,
    item_feature.tags,
    item_feature.tags_len,
    item_feature.publish_time,
    item_feature.price,
    COALESCE(item_feature_day_before.item_expose_cnt_1d, 0),
    COALESCE(item_feature_day_before.item_click_cnt_1d, 0),
    COALESCE(item_feature_day_before.item_cart_uv_15d, 0),
    COALESCE(item_feature_day_before.item_order_uv_30d, 0),
    COALESCE(item_feature_day_before.item_click_rate_1d, 0),
    CONCAT(user_feature.country, '_', rank_sample.item_id) AS country_item_id
FROM rank_sample_table rank_sample
INNER JOIN 
    user_feature_table user_feature
ON 
    rank_sample.user_id = user_feature.user_id
INNER JOIN 
    item_feature_table item_feature
ON 
    rank_sample.item_id = item_feature.item_id
LEFT JOIN 
    user_feature_table user_feature_day_before
ON 
    rank_sample.user_id = user_feature_day_before.user_id 
    AND user_feature_day_before.ds = to_char(date_sub(to_date('${bdp.system.bizdate}', 'yyyymmdd'),1), 'yyyymmdd')
LEFT JOIN 
    item_feature_table item_feature_day_before
ON 
    rank_sample.item_id = item_feature_day_before.item_id 
    AND item_feature_day_before.ds = to_char(date_sub(to_date('${bdp.system.bizdate}', 'yyyymmdd'),1), 'yyyymmdd')
WHERE 
    rank_sample.ds = '${bdp.system.bizdate}' 
    AND user_feature.ds = '${bdp.system.bizdate}' 
    AND item_feature.ds = '${bdp.system.bizdate}';