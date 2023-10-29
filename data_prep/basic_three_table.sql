CREATE TABLE IF NOT EXISTS `item_table`
(
    item_id STRING COMMENT "内容唯一标识ID"
    ,cate_id_1 bigint COMMENT "一级类目"
    ,cate_id_2 bigint COMMENT "二级类目"
    ,cate_id_3 bigint COMMENT "三级类目"
    ,shop_id STRING COMMENT "商店id"
    ,publish_time STRING COMMENT "发布时间"
    ,title STRING COMMENT "内容标题"
    ,tags STRING COMMENT "标签"
    ,status STRING COMMENT "是否可推荐"
    ,price bigint COMMENT "原始价格"
    ,expose_cnt_1d bigint comment "近一天曝光次数"
    ,click_cnt_1d bigint COMMENT "近一天点击次数"
    ,cart_uv_15d bigint COMMENT "近十五天加购人数"
    ,order_uv_30d bigint COMMENT "近三十天下单人数"
)
PARTITIONED BY
(
    ds STRING
)
LIFECYCLE 365
;

CREATE TABLE IF NOT EXISTS `user_table`
(
    user_id STRING COMMENT "用户唯一ID"
    ,device_id STRING COMMENT "用户设备ID"
    ,country STRING COMMENT "国家"
    ,city STRING COMMENT "城市"
    ,register_time bigint COMMENT "注册时间戳"
    ,last_login_time bigint COMMENT "上次登录时间戳"
    ,device_model STRING COMMENT "设备型号"
    ,device_language STRING comment "设备语言"
    ,os STRING comment "操作系统"
    ,media_source STRING COMMENT "用户来源"
    ,campaign STRING COMMENT "投放广告活动 campaign"
    ,tags STRING COMMENT "用户tags"
    ,user_level bigint COMMENT "用户等级"
    ,active_1d bigint comment "近一天访问天数"
    ,active_30d bigint comment "近三十天访问天数"
    ,expose_cnt_1d bigint comment "近一天曝光次数"
    ,click_cnt_1d bigint COMMENT "近一天点击次数"
    ,cart_cnt_15d bigint COMMENT "近十五天加购次数"
    ,order_cnt_30d bigint COMMENT "近三十天下单次数"
)
PARTITIONED BY
(
    ds STRING
)
LIFECYCLE 365
;

CREATE TABLE IF NOT EXISTS `behavior_table`
(
    user_id STRING COMMENT "用户ID"
    ,device_id STRING COMMENT "设备ID"
    ,item_id STRING COMMENT "内容ID"
    ,spm STRING COMMENT "页面位置信息"
    ,scm STRING COMMENT "内容来源信息"
    ,trace_id STRING COMMENT "内容来源详细埋点"
    ,country STRING COMMENT "国家"
    ,city STRING COMMENT "城市"
    ,os STRING comment "操作系统"
    ,pos bigint COMMENT "商品所在的位置信息"
    ,device_model STRING COMMENT "设备型号"
    ,bhv_type STRING COMMENT "行为类型"
    ,bhv_value STRING COMMENT "行为详情"
    ,bhv_time bigint COMMENT "行为发生的时间戳"
    ,query STRING COMMENT "搜索query"
    ,session_id STRING COMMENT "会话ID"
)
PARTITIONED BY 
(
    ds STRING
)
LIFECYCLE 90
;