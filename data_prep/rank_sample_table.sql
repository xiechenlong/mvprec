-- 创建排序模型样本表
CREATE TABLE IF NOT EXISTS `rank_sample_table`
(
    user_id STRING COMMENT "用户ID",
    item_id STRING COMMENT "内容ID",
    trace_id STRING COMMENT "内容来源详细埋点",
    bhv_time bigint COMMENT "行为发生的时间戳",
    country STRING COMMENT "国家",
    city STRING COMMENT "城市",
    os STRING COMMENT "操作系统",
    pos bigint COMMENT "商品所在的位置信息",
    device_model STRING COMMENT "设备型号",
    session_id STRING COMMENT "会话ID",
    is_click BIGINT COMMENT "是否点击",
    is_cart BIGINT COMMENT "是否加入购物车"
)
PARTITIONED BY (ds STRING)
LIFECYCLE 90;

-- 插入排序模型样本数据
INSERT OVERWRITE TABLE `rank_sample_table` PARTITION (ds='${bizdate}')
SELECT 
    e.user_id, 
    e.item_id, 
    e.trace_id, 
    e.bhv_time, 
    e.country, 
    e.city, 
    e.os, 
    e.pos, 
    e.device_model, 
    e.session_id,
    IF(c.user_id IS NOT NULL, 1, 0) as is_click,
    IF(ca.user_id IS NOT NULL, 1, 0) as is_cart
FROM (
    SELECT *,
    COUNT(*) OVER (PARTITION BY user_id) as expose_count,
    ROW_NUMBER() OVER (PARTITION BY user_id, item_id, get_json_object(trace_id,'$.request_id') ORDER BY bhv_time ASC) as rn
    FROM behavior_table_expose
    WHERE ds='${bizdate}' and bhv_value >= 100 -- 过滤掉停留时间少于100ms的曝光事件
) e
LEFT JOIN (
    SELECT *,
    COUNT(*) OVER (PARTITION BY user_id) as click_count,
    ROW_NUMBER() OVER (PARTITION BY user_id, item_id, get_json_object(trace_id,'$.request_id') ORDER BY bhv_time ASC) as rn
    FROM behavior_table_click
    WHERE ds='${bizdate}'
) c ON e.user_id = c.user_id AND e.item_id = c.item_id AND e.request_id = get_json_object(c.trace_id,'$.request_id') AND c.rn = 1
LEFT JOIN (
    SELECT *,
    COUNT(*) OVER (PARTITION BY user_id) as cart_count,
    ROW_NUMBER() OVER (PARTITION BY user_id, item_id, get_json_object(trace_id,'$.request_id') ORDER BY bhv_time ASC) as rn
    FROM behavior_table_cart
    WHERE ds='${bizdate}'
) ca ON e.user_id = ca.user_id AND e.item_id = ca.item_id AND e.request_id = get_json_object(ca.trace_id,'$.request_id') AND ca.rn = 1
LEFT JOIN (
    SELECT user_id, MAX(bhv_time) as last_click_time
    FROM behavior_table_click
    WHERE ds='${bizdate}'
    GROUP BY user_id
) lc ON e.user_id = lc.user_id 
WHERE e.rn = 1 -- 曝光去重
AND e.expose_count <= 1000 -- 过滤掉曝光次数超过1000次的用户
AND (c.click_count <= 100 OR c.click_count IS NULL) -- 过滤掉点击次数超过100次的用户
AND (ca.cart_count <= 100 OR ca.cart_count IS NULL) -- 过滤掉加购次数超过100次的用户
AND NOT (e.expose_count > 50 AND c.click_count IS NULL) -- 曝光次数大于50次，但没有点击行为的用户
AND (e.bhv_time <= lc.last_click_time OR lc.last_click_time IS NULL) -- 过滤掉最后一次点击行为之后的曝光记录
;