CREATE TABLE IF NOT EXISTS `recall_i2i_samples`
(
    user_id STRING COMMENT "用户ID",
    item_id STRING COMMENT "内容ID",
    group_id bigint COMMENT "分组ID"
)
PARTITIONED BY 
(
    ds STRING
)
LIFECYCLE 90
;

INSERT OVERWRITE TABLE recall_samples PARTITION (ds = ${bizdate})
SELECT 
    user_id, 
    item_id, 
    cate_id_2 AS group_id
FROM (
    SELECT 
        b.user_id, 
        b.item_id, 
        i.cate_id_2, 
        COUNT(*) OVER(PARTITION BY b.user_id) AS item_clicks, 
        ROW_NUMBER() OVER(PARTITION BY b.item_id ORDER BY RAND()) AS row
    FROM (
        SELECT DISTINCT
            user_id,
            item_id
        FROM
            behavior_table_click
        WHERE 
            ds BETWEEN to_char(date_sub(TO_DATE(${bizdate}, 'yyyymmdd'), 2),'yyyymmdd') AND ${bizdate}
            AND bhv_type = 'click'
    ) b 
    JOIN 
        item_table i ON b.item_id = i.item_id AND b.ds = i.ds
    WHERE 
        i.ds = ${bizdate}
) tmp 
WHERE 
    item_clicks BETWEEN 5 AND 1000
    AND row <= 10000
;