create database dev_realtime_360_4_chenming;
use dev_realtime_360_4_chenming;


drop table dev_realtime_360_4_chenming.ods_product_base;
CREATE external TABLE IF NOT EXISTS dev_realtime_360_4_chenming.ods_product_base (
    `sku_id` BIGINT  COMMENT '商品SKU ID',
    `category_id` BIGINT  COMMENT '所属品类ID',
    `category_name` VARCHAR(255) COMMENT '品类名称',
    `product_name` VARCHAR(500) COMMENT '商品名称',
    `price` DECIMAL(10,2) COMMENT '商品单价',
    `attribute_json` STRING COMMENT '商品属性（JSON格式，如{"颜色":"红色","尺寸":"XL"}）',
    `create_time` string COMMENT '商品上架时间',
    `update_time` string COMMENT '最后修改时间'
)
COMMENT '商品基础信息表（原始数据）'
    location '/2207A/chenming/360KBan4/ods/ods_product_base/'
tblproperties ("orc.compress"="snappy");

-- 工单编号：订单事实表（ods_order_fact）
drop table dev_realtime_360_4_chenming.ods_order_fact;
CREATE external TABLE  ods_order_fact (
    `order_id` VARCHAR(50) COMMENT '订单ID',
    `user_id` BIGINT  COMMENT '用户ID',
    `sku_id` BIGINT  COMMENT '商品SKU ID',
    `category_id` BIGINT COMMENT '订单商品所属品类ID',
    `payment_amount` DECIMAL(18,2) COMMENT '实际支付金额',
    `order_time` string COMMENT '下单时间',
    `payment_time` string COMMENT '支付时间',
    `status` string COMMENT '订单状态（0-取消，1-已支付）',
    `province` VARCHAR(50) COMMENT '收货省份',
    `city` VARCHAR(50) COMMENT '收货城市'
)
COMMENT '订单原始事实表（未聚合的明细数据）'
    location '/2207A/chenming/360KBan4/ods/ods_order_fact/'
tblproperties ("orc.compress"="snappy");

-- 工单编号：用户行为日志表（ods_user_behavior_log）
drop table  dev_realtime_360_4_chenming.ods_user_behavior_log;
CREATE external TABLE ods_user_behavior_log (
    `log_id` BIGINT  COMMENT '日志ID',
    `user_id` BIGINT COMMENT '用户ID',
    `sku_id` BIGINT COMMENT '商品SKU ID',
    `category_id` BIGINT COMMENT '品类ID',
    `behavior_type` STRING COMMENT '行为类型（取值：search/click/add_cart/purchase）',
    `channel` VARCHAR(50) COMMENT '行为渠道（如搜索、推荐、直通车）',
    `keyword` VARCHAR(255) COMMENT '搜索关键词（仅搜索行为有效）',
    `event_time` string COMMENT '行为时间',
    `device` VARCHAR(50) COMMENT '设备类型（APP/PC）',
    `ip` VARCHAR(50) COMMENT '用户IP地址'
)
COMMENT '用户行为原始日志表（未清洗的明细数据）'
    location '/2207A/chenming/360KBan4/ods/ods_user_behavior_log/'
tblproperties ("orc.compress"="snappy");

-- . . 流量访问日志表（ods_traffic_log）
drop table  dev_realtime_360_4_chenming.ods_traffic_log;
CREATE external TABLE ods_traffic_log (
    `log_id` BIGINT  COMMENT '日志ID',
    `session_id` VARCHAR(100) COMMENT '会话ID',
    `user_id` BIGINT COMMENT '用户ID（未登录为NULL）',
    `sku_id` BIGINT COMMENT '商品SKU ID',
    `category_id` BIGINT COMMENT '品类ID',
    `page_url` VARCHAR(500) COMMENT '访问页面URL',
    `referrer` VARCHAR(500) COMMENT '流量来源（如外部链接、广告）',
    `event_time` string COMMENT '访问时间',
    `stay_duration` INT COMMENT '页面停留时长（秒）',
    `device` VARCHAR(50) COMMENT '设备类型（APP/PC）'
)
COMMENT '流量访问原始日志表（未聚合的明细数据）'
    location '/2207A/chenming/360KBan4/ods/ods_traffic_log/'
tblproperties ("orc.compress"="snappy");

-- 工单编号 商品属性维度表（ods_product_attribute）
drop table dev_realtime_360_4_chenming.ods_product_attribute;
CREATE external TABLE   ods_product_attribute (
    `attribute_id` BIGINT  COMMENT '属性ID',
    `category_id` BIGINT COMMENT '品类ID',
    `attribute_key` VARCHAR(100) COMMENT '属性名称（如颜色、尺寸）',
    `attribute_value` VARCHAR(100) COMMENT '属性值（如红色、XL）',
    `is_active` string COMMENT '是否生效（0-失效，1-生效）'
)
COMMENT '商品属性维度表（原始配置数据）'
    location '/2207A/chenming/360KBan4/ods/ods_product_attribute/'
tblproperties ("orc.compress"="snappy");



--订单事实表（dwd_order_fact）
drop table dev_realtime_360_4_chenming.dwd_order_fact;
CREATE external TABLE  dwd_order_fact (
    `order_id` string COMMENT '订单ID',
    `user_id` BIGINT  COMMENT '用户ID',
    `sku_id` BIGINT  COMMENT '商品SKU ID',
    `category_id` BIGINT COMMENT '订单商品所属品类ID',
    `payment_amount` DECIMAL(18,2) COMMENT '实际支付金额',
    `order_time` string COMMENT '下单时间',
    `payment_time` string COMMENT '支付时间',
    `status` string COMMENT '订单状态（0-取消，1-已支付）',
    `province` VARCHAR(50) COMMENT '收货省份',
    `city` VARCHAR(50) COMMENT '收货城市'
)
COMMENT '订单原始事实表（未聚合的明细数据）'
    PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan4/dwd/dwd_order_fact/'
tblproperties ("orc.compress"="snappy");



--商品明细信息表（dwd_product_base）
CREATE external TABLE IF NOT EXISTS dev_realtime_360_4_chenming.dwd_product_base (
    `sku_id` BIGINT  COMMENT '商品SKU ID',
    `category_id` BIGINT  COMMENT '所属品类ID',
    `category_name` VARCHAR(255) COMMENT '品类名称',
    `product_name` VARCHAR(500) COMMENT '商品名称',
    `price` DECIMAL(10,2) COMMENT '商品单价',
    `attribute_json` STRING COMMENT '商品属性（JSON格式，如{"颜色":"红色","尺寸":"XL"}）',
    `create_time` string COMMENT '商品上架时间',
    `update_time` string COMMENT '最后修改时间'
)
COMMENT '商品基础信息表（原始数据）'
     PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan4/dwd/dwd_product_base/'
tblproperties ("orc.compress"="snappy");

-- 工单编号：用户行为日志表（dwd_user_behavior_log）
drop table  dev_realtime_360_4_chenming.dwd_user_behavior_log;
CREATE external TABLE dwd_user_behavior_log (
    `log_id` BIGINT  COMMENT '日志ID',
    `user_id` BIGINT COMMENT '用户ID',
    `sku_id` BIGINT COMMENT '商品SKU ID',
    `category_id` BIGINT COMMENT '品类ID',
    `behavior_type` STRING COMMENT '行为类型（取值：search/click/add_cart/purchase）',
    `channel` VARCHAR(50) COMMENT '行为渠道（如搜索、推荐、直通车）',
    `keyword` VARCHAR(255) COMMENT '搜索关键词（仅搜索行为有效）',
    `event_time` string COMMENT '行为时间',
    `device` VARCHAR(50) COMMENT '设备类型（APP/PC）',
    `ip` VARCHAR(50) COMMENT '用户IP地址'
)
COMMENT '用户行为原始日志表（未清洗的明细数据）'
         PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan4/ods/ods_user_behavior_log/'
tblproperties ("orc.compress"="snappy");


----流量访问日志表
drop table  dev_realtime_360_4_chenming.dwd_traffic_log;
CREATE external TABLE dwd_traffic_log (
    `log_id` BIGINT  COMMENT '日志ID',
    `session_id` VARCHAR(100) COMMENT '会话ID',
    `user_id` BIGINT COMMENT '用户ID（未登录为NULL）',
    `sku_id` BIGINT COMMENT '商品SKU ID',
    `category_id` BIGINT COMMENT '品类ID',
    `page_url` VARCHAR(500) COMMENT '访问页面URL',
    `referrer` VARCHAR(500) COMMENT '流量来源（如外部链接、广告）',
    `event_time` string COMMENT '访问时间',
    `stay_duration` INT COMMENT '页面停留时长（秒）',
    `device` VARCHAR(50) COMMENT '设备类型（APP/PC）'
)
COMMENT '流量访问原始日志表（未聚合的明细数据）'
 PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan4/dwd/dwd_traffic_log/'
tblproperties ("orc.compress"="snappy");


 --商品属性维度表（dwd_product_attribute）
drop table dev_realtime_360_4_chenming.dwd_product_attribute;
CREATE external TABLE   dwd_product_attribute (
    `attribute_id` BIGINT  COMMENT '属性ID',
    `category_id` BIGINT COMMENT '品类ID',
    `attribute_key` VARCHAR(100) COMMENT '属性名称（如颜色、尺寸）',
    `attribute_value` VARCHAR(100) COMMENT '属性值（如红色、XL）',
    `is_active` string COMMENT '是否生效（0-失效，1-生效）'
)
COMMENT '商品属性维度表（原始配置数据）'
     PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan4/dwd/dwd_product_attribute/'
tblproperties ("orc.compress"="snappy");



insert into table dwd_order_fact partition (ds = '2025-04-01')
select  *from ods_order_fact;

insert into table dwd_product_base partition (ds = '2025-04-01')
select  *from ods_product_base;

insert into table dwd_user_behavior_log partition (ds = '2025-04-01')
select  *from ods_user_behavior_log;

insert into table dwd_traffic_log partition (ds = '2025-04-01')
select  *from ods_traffic_log;

insert into table dwd_product_attribute partition (ds = '2025-04-01')
select  *from ods_product_attribute;


-- 工单编号： 销售分析表（分层设计）
drop table ads_sales_analysis;
CREATE TABLE IF NOT EXISTS ads_sales_analysis (
                                                  `category_id` BIGINT COMMENT '品类ID',
                                                  `category_name` VARCHAR(255) COMMENT '品类名称',
    `gmv` DECIMAL(18,2) COMMENT '总销售额',
    `order_count` BIGINT COMMENT '总销量',
    `time_granularity` string COMMENT '时间维度（日/周/月）',
    `top_sku_json` string COMMENT '热销商品排名（JSON格式，如[{"sku_id":1,"sales":1000},...]）',
    ds string
    )
    COMMENT '销售分析表-按时间维度聚合品类销售数据（分层设计）'
    location '/2207A/chenming/360KBan/ads/ads_sales_analysis'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );

INSERT OVERWRITE TABLE ads_sales_analysis
SELECT
    category_id,
    category_name AS category_name,
    SUM(total_payment) AS gmv,          -- 修正：从子查询获取总支付金额
    COUNT(DISTINCT order_id) AS order_count,
    'day' AS time_granularity,
    CONCAT(
            '[',
            IF(
                    COLLECT_LIST(json_entry) IS NOT NULL,
                    CONCAT_WS(',', COLLECT_LIST(json_entry)),
                    ''
                ),
            ']'
        ) AS top_sku_json,
    '2025-04-01' AS ds
FROM (
         SELECT
             o.category_id,
             p.category_name,
             o.sku_id,
             o.order_id,                       -- 保留订单ID用于外层去重
             COUNT(DISTINCT o.order_id) AS sales_count,
             SUM(o.payment_amount) AS total_payment,  -- 新增：计算每个SKU的总支付金额
             CONCAT(
                     '{"sku_id":',
                     o.sku_id,
                     ',"sales":',
                     COUNT(DISTINCT o.order_id),
                     '}'
                 ) AS json_entry
         FROM dwd_order_fact o
                  JOIN dwd_product_base p
                       ON o.sku_id = p.sku_id
                           AND o.ds = '2025-04-01'           -- 分区条件写入JOIN提升性能
                           AND p.ds = '2025-04-01'
         WHERE o.status = '1'                -- 确保status字段类型匹配（若为数值需改为1）
         GROUP BY o.category_id, o.sku_id, p.category_name, o.order_id
     ) t
GROUP BY category_id,category_name;


-- 工单编号：属性分析表（不分层设计）
drop table ads_attribute_analysis;
CREATE TABLE IF NOT EXISTS ads_attribute_analysis (
                                                      `category_id` BIGINT COMMENT '品类ID',
                                                      `attribute_key` VARCHAR(255) COMMENT '属性名称（如颜色、尺寸）',
    `attribute_value` VARCHAR(255) COMMENT '属性值（如红色、XL）',
    `traffic_uv` BIGINT COMMENT '属性流量（访客数）',
    `conversion_rate` DECIMAL(5,2) COMMENT '支付转化率（百分比）',
    `active_sku_count` BIGINT COMMENT '动销商品数',
    ds string COMMENT '统计日期'

    )
    COMMENT '属性分析表-按属性维度统计流量与转化（不分层设计）'
    location '/2207A/chenming/360KBan/ads/ads_attribute_analysis'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );


INSERT OVERWRITE TABLE ads_attribute_analysis
SELECT
    a.category_id,
    a.attribute_key,
    a.attribute_value,
    -- 计算属性流量（访客数）
    COUNT(DISTINCT l.user_id) AS traffic_uv,
    -- 计算支付转化率（处理分母为 0 的情况）
    ROUND(
            CASE
                WHEN COUNT(DISTINCT l.user_id) = 0 THEN 0.00
                ELSE COUNT(DISTINCT CASE WHEN o.status = '1' THEN o.user_id END) * 100.0 / COUNT(DISTINCT l.user_id)
                END,
            2
        ) AS conversion_rate,
    -- 计算动销商品数（处理无订单的空值情况）
    COALESCE(COUNT(DISTINCT o.sku_id), 0) AS active_sku_count,
    '2025-04-01' AS ds
FROM
    dwd_product_attribute a
-- 关联用户行为日志（假设关注搜索行为）
        LEFT JOIN dwd_user_behavior_log l
                  ON a.category_id = l.category_id
                      AND l.ds = '2025-04-01'
                      AND l.behavior_type = 'search'
-- 关联订单表（假设 '1' 为已支付状态）
        LEFT JOIN dwd_order_fact o
                  ON a.category_id = o.category_id
                      AND o.ds = '2025-04-01'
                      AND o.status = '1'
WHERE
        a.ds = '2025-04-01'  -- 过滤属性表统计日期
GROUP BY
    a.category_id,
    a.attribute_key,
    a.attribute_value;




-- 工单编号：流量分析表（分层设计）
drop table ads_traffic_analysis;
CREATE TABLE IF NOT EXISTS ads_traffic_analysis (
                                                    `month` DATE COMMENT '统计月份',
                                                    `category_id` BIGINT COMMENT '品类ID',
                                                    `channel` VARCHAR(50) COMMENT '流量渠道（如搜索、直通车、推荐）',
    `traffic_pv` BIGINT COMMENT '渠道访问量',
    `traffic_uv` BIGINT COMMENT '渠道访客数',
    `hot_search_keywords` string COMMENT '热搜词（JSON格式，如[{"keyword":"手机","uv":1000},...]）'
    )
    COMMENT '流量分析表-按月统计品类流量来源与热搜词（分层设计）'
    location '/2207A/chenming/360KBan/ads/ads_traffic_analysis'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );


INSERT OVERWRITE TABLE ads_traffic_analysis
SELECT
    month,
    category_id,
    channel,
    COUNT(log_id) AS traffic_pv,
    COUNT(DISTINCT user_id) AS traffic_uv,
    CONCAT(
    '[',
    CONCAT_WS(
    ',',
    COLLECT_LIST(
    CONCAT('{"keyword":"', REPLACE(keyword, '"', '\\"'), '","uv":', uv, '}')
    )
    ),
    ']'
    ) AS hot_search_keywords
FROM (
    SELECT
    t.log_id,
    t.category_id,
    b.channel,  -- 确保子查询中选择了 channel 列
    t.user_id,
    DATE_FORMAT(t.event_time, 'yyyy-MM-01') AS month,
    b.keyword,
    COUNT(DISTINCT b.user_id) AS uv
    FROM dwd_traffic_log t
    LEFT JOIN dwd_user_behavior_log b
    ON t.user_id = b.user_id
    AND b.behavior_type = 'search'
    WHERE t.ds = '2025-04-01'
    GROUP BY t.category_id, b.channel, b.keyword, t.log_id, t.user_id, t.event_time
    ) sub
GROUP BY month, category_id, channel;
-- 工单编号：客群洞察表（不分层设计）
CREATE TABLE IF NOT EXISTS ads_customer_insight (
                                                    `category_id` BIGINT COMMENT '品类ID',
                                                    `behavior_type` string COMMENT '用户行为类型（搜索/访问/支付）', --('search', 'visit', 'purchase')
                                                    `gender_dist` string COMMENT '性别分布（JSON格式，如{"male":60,"female":40}）',
                                                    `age_dist` string COMMENT '年龄分布（JSON格式，如{"18-25":30,"26-35":50}）',
                                                    `city_top10` string COMMENT '城市TOP10（JSON格式，如[{"city":"北京","uv":1000},...]）',
                                                    ds string
)
    COMMENT '客群洞察表-按月统计用户画像（不分层设计）'
    location '/2207A/chenming/360KBan/ads/ads_customer_insight'
    TBLPROPERTIES (
    'orc.compress = snappy'
                  );




INSERT OVERWRITE TABLE ads_customer_insight
SELECT
    category_id,
    behavior_type,
    CONCAT('{"male":', SUM(male), ',"female":', SUM(female), '}') AS gender_dist,
    CONCAT('{"18-25":', SUM(age_18_25), ',"26-35":', SUM(age_26_35), '}') AS age_dist,
    CONCAT(
            '[',
            IF(
                    COLLECT_LIST(city_entry) IS NOT NULL,
                    CONCAT_WS(',', COLLECT_LIST(city_entry)),
                    ''
                ),
            ']'
        ) AS city_top10,
    '2025-04-01' AS ds
FROM (
         SELECT
             l.category_id,
             l.behavior_type,
             -- 聚合字段需在外层 GROUP BY 后汇总
             SUM(CASE WHEN u.gender = 'male' THEN 1 ELSE 0 END) AS male,
             SUM(CASE WHEN u.gender = 'female' THEN 1 ELSE 0 END) AS female,
             SUM(CASE WHEN u.age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
             SUM(CASE WHEN u.age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
             o.city,
             COUNT(DISTINCT l.user_id) AS uv,
             -- 显式定义 JSON 条目
             CONCAT('{"city":"', o.city, '","uv":', COUNT(DISTINCT l.user_id), '}') AS city_entry
         FROM dwd_user_behavior_log l
                  JOIN (
             SELECT
                 user_id,
                 ELT(FLOOR(RAND()*2) + 1, 'male', 'female') AS gender,  -- 修复括号闭合
                 FLOOR(18 + RAND()*20) AS age
             FROM dwd_user_behavior_log
             WHERE ds = '2025-04-01'
             GROUP BY user_id
         ) u ON l.user_id = u.user_id
                  LEFT JOIN dwd_order_fact o
                            ON l.user_id = o.user_id
                                AND o.ds = '2025-04-01'
         WHERE l.ds = '2025-04-01'
         GROUP BY l.category_id, l.behavior_type, o.city
     ) t
GROUP BY category_id, behavior_type;  -- 外层按品类和行为类型分组


-- 综合看板表创建语句

-- 综合看板表创建语句
CREATE TABLE IF NOT EXISTS ads_dashboard (
                                             category_id BIGINT COMMENT '品类ID',
                                             category_name VARCHAR(255) COMMENT '品类名称',
    gmv DECIMAL(18,2) COMMENT '总销售额',
    order_count BIGINT COMMENT '总订单量',
    top_skus STRING COMMENT '热销商品TOP3（JSON数组）',
    top_attribute_key VARCHAR(255) COMMENT '最热门属性名称',
    top_attribute_value VARCHAR(255) COMMENT '最热门属性值',
    attribute_traffic_uv BIGINT COMMENT '属性流量UV',
    attribute_conversion_rate DECIMAL(5,2) COMMENT '属性转化率',
    active_sku_count BIGINT COMMENT '动销商品数',
    main_channel VARCHAR(50) COMMENT '主要流量渠道',
    channel_traffic_pv BIGINT COMMENT '渠道访问量',
    hot_keywords STRING COMMENT '热搜词TOP3',
    gender_dist STRING COMMENT '性别分布',
    age_dist STRING COMMENT '年龄分布',
    city_top10 STRING COMMENT '城市TOP10',
    ds STRING COMMENT '统计日期'
    )
    COMMENT '综合看板表-整合各维度关键指标'
    location '/2207A/chenming/360KBan/ads/ads_dashboard'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );

-- 综合看板表创建语句
CREATE TABLE IF NOT EXISTS ads_dashboard (
                                             category_id BIGINT COMMENT '品类ID',
                                             category_name VARCHAR(255) COMMENT '品类名称',
    gmv DECIMAL(18,2) COMMENT '总销售额',
    order_count BIGINT COMMENT '总订单量',
    top_skus STRING COMMENT '热销商品TOP3（JSON数组）',
    top_attribute_key VARCHAR(255) COMMENT '最热门属性名称',
    top_attribute_value VARCHAR(255) COMMENT '最热门属性值',
    attribute_traffic_uv BIGINT COMMENT '属性流量UV',
    attribute_conversion_rate DECIMAL(5,2) COMMENT '属性转化率',
    active_sku_count BIGINT COMMENT '动销商品数',
    main_channel VARCHAR(50) COMMENT '主要流量渠道',
    channel_traffic_pv BIGINT COMMENT '渠道访问量',
    hot_keywords STRING COMMENT '热搜词TOP3',
    gender_dist STRING COMMENT '性别分布',
    age_dist STRING COMMENT '年龄分布',
    city_top10 STRING COMMENT '城市TOP10（',
    ds STRING COMMENT '统计日期'
    )
    COMMENT '综合看板表-整合各维度关键指标'
    location '/2207A/chenming/360KBan/ads/ads_dashboard'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );

-- 数据加载语句
INSERT OVERWRITE TABLE ads_dashboard
SELECT
    COALESCE(s.category_id, a.category_id, t.category_id, c.category_id) AS category_id,
    s.category_name,
    s.gmv,
    s.order_count,
    s.top_skus,
    a.top_attribute_key,
    a.top_attribute_value,
    a.attribute_traffic_uv,
    a.attribute_conversion_rate,
    a.active_sku_count,
    t.main_channel,
    t.channel_traffic_pv,
    t.hot_keywords,
    c.gender_dist,
    c.age_dist,
    c.city_top10,
    '2025-04-01' AS ds
FROM (
         SELECT
             category_id,
             category_name,
             gmv,
             order_count,
             top_sku_json AS top_skus
         FROM ads_sales_analysis
         WHERE ds = '2025-04-01'
     ) s
         FULL OUTER JOIN (
    SELECT
        category_id,
        attribute_key AS top_attribute_key,
        attribute_value AS top_attribute_value,
        traffic_uv AS attribute_traffic_uv,
        conversion_rate AS attribute_conversion_rate,
        active_sku_count
    FROM (
             SELECT
                 *,
                 ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY traffic_uv DESC) AS rank
             FROM ads_attribute_analysis
             WHERE ds = '2025-04-01'
         ) t WHERE rank = 1
) a ON s.category_id = a.category_id
         FULL OUTER JOIN (
    SELECT
        category_id,
        channel AS main_channel,
        traffic_pv AS channel_traffic_pv,
        traffic_uv AS channel_traffic_uv,
        hot_search_keywords AS hot_keywords
    FROM (
             SELECT
                 *,
                 ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY traffic_uv DESC) AS rank
             FROM ads_traffic_analysis
             WHERE month = '2025-04-01'
         ) t WHERE rank = 1
) t ON s.category_id = t.category_id AND a.category_id = t.category_id
         FULL OUTER JOIN (
    SELECT
        category_id,
        CONCAT('{"male":', SUM(male), ',"female":', SUM(female), '}') AS gender_dist,
        CONCAT('{"18-25":', SUM(age_18_25), ',"26-35":', SUM(age_26_35), '}') AS age_dist,
        CONCAT('[', CONCAT_WS(',', COLLECT_LIST(city_entry)), ']') AS city_top10
    FROM (
             SELECT
                 l.category_id,
                 SUM(CASE WHEN u.gender = 'male' THEN 1 ELSE 0 END) AS male,
                 SUM(CASE WHEN u.gender = 'female' THEN 1 ELSE 0 END) AS female,
                 SUM(CASE WHEN u.age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
                 SUM(CASE WHEN u.age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
                 COUNT(DISTINCT l.user_id) AS uv,
                 CONCAT('{"city":"', o.city, '","uv":', COUNT(DISTINCT l.user_id), '}') AS city_entry
             FROM dwd_user_behavior_log l
                      JOIN (
                 SELECT
                     user_id,
                     ELT(FLOOR(RAND()*2) + 1, 'male', 'female') AS gender,
                     FLOOR(18 + RAND()*20) AS age
                 FROM dwd_user_behavior_log
                 WHERE ds = '2025-04-01'
                 GROUP BY user_id
             ) u ON l.user_id = u.user_id
                      LEFT JOIN dwd_order_fact o
                                ON l.user_id = o.user_id
                                    AND o.ds = '2025-04-01'
             WHERE l.ds = '2025-04-01'
             GROUP BY l.category_id, o.city
         ) t
    GROUP BY category_id
) c ON s.category_id = c.category_id AND a.category_id = c.category_id AND t.category_id = c.category_id;


--新表



