-- 工单编号：大数据-电商数仓-03-商品主题商品360看板
-- ODS层存储原始业务数据（未加工）
-- 1.创外部表 才可以locthon
-- 2.表名前面得加数据库名
-- 3.select * 必须换行
-- 4.加载orc 压缩-- snappy 格式
-- 5. dt 换成 ds
-- 商品基础信息表（全量快照）
drop  table  dev_realtime_test_chenming.ods_product_base;
CREATE external TABLE IF NOT EXISTS dev_realtime_test_chenming.ods_product_base (
  product_id     STRING COMMENT '商品ID',
  product_name   STRING COMMENT '商品名称',
  category_id    STRING COMMENT '类目ID',
  create_time    TIMESTAMP COMMENT '上架时间',
  price          DOUBLE COMMENT '商品价格'
)
COMMENT '商品基础信息原始表'
PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan/ods/ods_product_base/'
tblproperties ("orc.compress"="snappy");
-- tblproperties ("parquet.compression"="snappy" );


INSERT INTO dev_realtime_test_chenming.ods_product_base PARTITION(ds='2023-10-01')
SELECT
    concat('P', cast(1000 + seq AS STRING)) AS product_id,
    CASE WHEN rand() < 0.3 THEN '羽绒服'
         WHEN rand() < 0.6 THEN '羊毛大衣'
         ELSE '休闲裤' END AS product_name,
    concat('C', cast(floor(rand()*5)+1 AS STRING)) AS category_id,
    from_unixtime(unix_timestamp('2023-09-01') + floor(rand()*2592000)) AS create_time,
    round(100 + (rand()*900), 1) AS price
FROM (
         SELECT posexplode(split(space(1000), ' ')) AS (seq, x) -- 修正：补充分割符参数
     ) tmp
    LIMIT 1000;

-- SKU信息表
drop  table  dev_realtime_test_chenming.ods_product_sku;
CREATE external TABLE IF NOT EXISTS dev_realtime_test_chenming.ods_product_sku (
  sku_id         STRING COMMENT 'SKU唯一标识',
  product_id     STRING COMMENT '商品ID',
  attribute      STRING COMMENT '属性（如颜色/尺寸）',
  stock          INT    COMMENT '库存'
)
COMMENT 'SKU基础信息表'
PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan/ods/ods_product_sku/'
tblproperties ("orc.compress"="snappy");

INSERT INTO ods_product_sku PARTITION(ds='2023-10-01')
SELECT
    concat('SKU', lpad(cast(seq AS STRING), 3, '0')) AS sku_id, -- 修正：补充lpad参数
    product_id,
    concat(
            CASE WHEN rand() < 0.5 THEN '黑色' ELSE '白色' END,
            '/',
            CASE WHEN rand() < 0.5 THEN 'M' ELSE 'XL' END
        ) AS attribute,
    floor(50 + rand()*150) AS stock
FROM (
         SELECT product_id, (row_number() OVER () - 1) % 4 AS seq -- 修正：明确子查询别名
         FROM ods_product_base
         WHERE ds='2023-10-01'
     ) sku_gen;

-- 订单明细表（增量数据）
drop  table  dev_realtime_test_chenming.ods_order_detail;
CREATE external TABLE IF NOT EXISTS dev_realtime_test_chenming.ods_order_detail (
  order_id       STRING COMMENT '订单ID',
  user_id        STRING COMMENT '用户ID',
  product_id     STRING COMMENT '商品ID',
  sku_id         STRING COMMENT 'SKUID',
  quantity       INT    COMMENT '购买数量',
  amount         DOUBLE COMMENT '实际支付金额',
  event_time     TIMESTAMP COMMENT '下单时间'
)
COMMENT '原始订单交易数据'
PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan/ods/ods_order_detail/'
tblproperties ("orc.compress"="snappy");

INSERT INTO ods_order_detail PARTITION(ds='2023-10-01')
SELECT
    concat('O', from_unixtime(unix_timestamp(), 'yyyyMMdd'), lpad(cast(seq AS STRING), 4, '0')) AS order_id,
    concat('U', cast(floor(rand()*1000)+1 AS STRING)) AS user_id,
    product_id,
    sku_id,
    quantity, -- 从子查询中获取已计算的 quantity
    round(price * quantity * (0.9 + rand()*0.2), 2) AS amount, -- 直接使用子查询中的 quantity
    from_unixtime(unix_timestamp('2023-10-01 00:00:00') + floor(rand()*86400)) AS event_time
FROM (
         SELECT
             p.product_id,
             s.sku_id,
             p.price,
             floor(1 + rand()*3) AS quantity, -- 将 quantity 计算移至子查询
             row_number() OVER () AS seq
         FROM ods_product_base p
                  JOIN ods_product_sku s ON p.product_id = s.product_id
         WHERE p.ds='2023-10-01' AND s.ds='2023-10-01'
     ) order_gen
    LIMIT 1000;
-- 用户行为日志表
drop  table  dev_realtime_test_chenming.ods_user_behavior_log;
CREATE external TABLE IF NOT EXISTS dev_realtime_test_chenming.ods_user_behavior_log (
  log_id         STRING COMMENT '日志ID',
  user_id        STRING COMMENT '用户ID',
  product_id     STRING COMMENT '商品ID',
  behavior_type  STRING COMMENT '行为类型（click/view/cart/pay）',
  channel        STRING COMMENT '流量渠道（搜索/推荐/直播）',
  event_time     TIMESTAMP COMMENT '行为时间'
)
COMMENT '用户行为原始日志'
PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan/ods/ods_user_behavior_log/'
tblproperties ("orc.compress"="snappy");

INSERT  INTO ods_user_behavior_log PARTITION(ds='2023-10-01')
SELECT
    concat('LOG', lpad(cast(seq AS STRING), 6, '0')) AS log_id,
    concat('U', cast(floor(rand()*1000)+1 AS STRING)) AS user_id,
    product_id,
    CASE WHEN rand() < 0.6 THEN 'click'
         WHEN rand() < 0.8 THEN 'view'
         ELSE 'cart' END AS behavior_type,
    CASE WHEN rand() < 0.5 THEN 'search' ELSE 'recommend' END AS channel,
    from_unixtime(unix_timestamp('2023-10-01 00:00:00') + floor(rand()*86400)) AS event_time
FROM (
         SELECT
             product_id,
             row_number() OVER () AS seq
         FROM ods_product_base
         WHERE ds='2023-10-01'
     ) log_gen
    LIMIT 1000;


-- 商品价格变动表
drop  table  dev_realtime_test_chenming.ods_price_change_log;
CREATE external TABLE IF NOT EXISTS dev_realtime_test_chenming.ods_price_change_log (
  product_id     STRING COMMENT '商品ID',
  old_price      DOUBLE COMMENT '原价格',
  new_price      DOUBLE COMMENT '新价格',
  operator       STRING COMMENT '操作人',
  change_time    TIMESTAMP COMMENT '修改时间'
)
COMMENT '商品价格变动流水表'
PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan/ods/ods_price_change_log/'
tblproperties ("orc.compress"="snappy");

-- 生成15条价格变动记录（每个商品随机1-2次调价）
INSERT INTO ods_price_change_log PARTITION(ds='2023-10-01')
SELECT
    p.product_id,
    p.price AS old_price,  -- 初始价格取自商品表
    round(p.price * (0.8 + rand()*0.4), 2) AS new_price, -- 价格波动范围80%~120%
    concat('admin', cast(floor(rand()*3)+1 AS STRING)) AS operator, -- 操作人admin1~admin3
    from_unixtime(unix_timestamp(p.create_time) + floor(rand()*86400*3)) AS change_time -- 在商品创建时间后3天内随机修改
FROM (
         SELECT
             product_id,
             price,
             create_time,
             row_number() OVER (PARTITION BY product_id ORDER BY rand()) AS rn
         FROM ods_product_base
         WHERE ds='2023-10-01'
     ) p
WHERE rn <= 2; -- 每个商品最多2次调价

-- 商品评价表
drop  table  dev_realtime_test_chenming.ods_product_review;
CREATE external TABLE IF NOT EXISTS dev_realtime_test_chenming.ods_product_review (
  review_id      STRING COMMENT '评价ID',
  product_id     STRING COMMENT '商品ID',
  user_id        STRING COMMENT '用户ID',
  score          INT    COMMENT '评分（1-5分）',
  content        STRING COMMENT '评价内容',
  create_time    TIMESTAMP COMMENT '评价时间'
)
COMMENT '商品评价原始数据'
PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan/ods/ods_product_review/'
tblproperties ("orc.compress"="snappy");
INSERT INTO ods_product_review PARTITION(ds='2023-10-01')
SELECT
    concat('R', lpad(cast(seq AS STRING), 4, '0')) AS review_id,
    product_id,
    concat('U', cast(floor(rand()*1000)+1 AS STRING)) AS user_id,
    score,
    CASE WHEN score >=4 THEN '质量很好，会回购'
         WHEN score=3 THEN '一般般'
         ELSE '做工粗糙' END AS content,
    from_unixtime(unix_timestamp('2023-10-01 00:00:00') + floor(rand()*86400)) AS create_time
FROM (
         SELECT
             product_id,
             row_number() OVER () AS seq,
                 floor(1 + rand()*5) AS score
         FROM ods_product_base
         WHERE ds='2023-10-01'
     ) review_gen
    LIMIT 20;

-- 运营活动表
drop  table  dev_realtime_test_chenming.ods_operation_activity;
CREATE external TABLE IF NOT EXISTS dev_realtime_test_chenming.ods_operation_activity (
  activity_id    STRING COMMENT '活动ID',
  product_id     STRING COMMENT '商品ID',
  action_type    STRING COMMENT '活动类型（首单礼金/短视频/主图修改）',
  start_time     TIMESTAMP COMMENT '开始时间',
  end_time       TIMESTAMP COMMENT '结束时间'
)
COMMENT '运营活动配置表'
PARTITIONED BY (ds STRING COMMENT '日期分区')
    location '/2207A/chenming/360KBan/ods/ods_operation_activity/'
tblproperties ("orc.compress"="snappy");

-- 生成10条运营活动数据（覆盖所有活动类型）
INSERT INTO ods_operation_activity PARTITION(ds='2023-10-01')
SELECT
    activity_id,
    product_id,
    action_type,
    start_time,
    from_unixtime(unix_timestamp(start_time) + floor(rand()*604800)) AS end_time
FROM (
         SELECT
             concat('ACT', lpad(cast(seq AS STRING), 4, '0')) AS activity_id,
             product_id,
             CASE WHEN rand() < 0.3 THEN '首单礼金'
                  WHEN rand() < 0.6 THEN '短视频'
                  ELSE '主图修改' END AS action_type,
             from_unixtime(unix_timestamp('2023-10-01') - floor(rand()*2592000)) AS start_time,
             seq
         FROM (
                  SELECT
                      p.product_id,
                      row_number() OVER () AS seq
                  FROM ods_product_base p
                  WHERE p.ds='2023-10-01'
                      LIMIT 10
              ) tmp
     ) activity_data;



-- 商品销售核心指标

-- 商品销售核心指标表（ADS层）
CREATE external TABLE IF NOT EXISTS ads_product_sales_core (
  product_id       STRING    COMMENT '商品ID',
  product_name     STRING    COMMENT '商品名称',
  category_id      STRING    COMMENT '类目ID',
  order_count      BIGINT    COMMENT '订单数量',
  sales_volume     BIGINT    COMMENT '销售总量',
  gmv             DOUBLE    COMMENT '成交总额',
  buyer_count      BIGINT    COMMENT '购买人数',
  avg_order_value DOUBLE    COMMENT '客单价',
  click_count      BIGINT    COMMENT '点击量',
  conversion_rate DOUBLE    COMMENT '转化率',
  ds              STRING    COMMENT '统计日期'
)
COMMENT '商品销售核心指标表'
location '/2207A/chenming/360KBan/ads/ads_product_sales_core'
TBLPROPERTIES (
  "orc.compress"="SNAPPY",
  "transactional"="false"
);

insert overwrite table ads_product_sales_core
SELECT

    p.product_id,
    p.product_name,
    p.category_id,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.quantity) AS sales_volume,
    SUM(o.amount) AS gmv,
    COUNT(DISTINCT o.user_id) AS buyer_count,
    SUM(o.amount) / COUNT(DISTINCT o.order_id) AS avg_order_value,
    COUNT(DISTINCT CASE WHEN u.behavior_type = 'click' THEN u.log_id END) AS click_count,
    COUNT(DISTINCT o.order_id) / COUNT(DISTINCT CASE WHEN u.behavior_type = 'click' THEN u.log_id END) AS conversion_rate,
    o.ds
FROM
    ods_product_base p
        LEFT JOIN
    ods_order_detail o ON p.product_id = o.product_id AND o.ds = '2023-10-01'
        LEFT JOIN
    ods_user_behavior_log u ON p.product_id = u.product_id AND u.ds = '2023-10-01'
WHERE
        p.ds = '2023-10-01' and o.ds = '2023-10-01'
GROUP BY
    p.product_id, p.product_name, p.category_id,o.ds;










-- 按天销售趋势
drop  table ads_product_sales_trend;
CREATE TABLE IF NOT EXISTS ads_product_sales_trend (
                                                       product_id         STRING    COMMENT '商品ID',
                                                       sale_date         STRING    COMMENT '销售日期',
                                                       daily_order_count BIGINT    COMMENT '日订单量',
                                                       daily_gmv        DOUBLE    COMMENT '日成交额',
                                                       ds               STRING    COMMENT '统计日期'
)
    COMMENT '商品销售趋势表'
    location '/2207A/chenming/360KBan/ads/ads_product_sales_trend'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );
insert overwrite table ads_product_sales_trend
SELECT

    p.product_id,
    date_format(o.event_time, 'yyyy-MM-dd') AS sale_date,
    COUNT(DISTINCT o.order_id) AS daily_order_count,
    SUM(o.amount) AS daily_gmv,
    o.ds
FROM
    ods_product_base p
        JOIN
    ods_order_detail o ON p.product_id = o.product_id
WHERE
        p.ds = '2023-10-01' AND
    o.ds BETWEEN '2023-09-25' AND '2023-10-01'  -- 最近7天数据
GROUP BY
    p.product_id, date_format(o.event_time, 'yyyy-MM-dd'),o.ds;





-- SKU销售分析表（ADS层）
drop  table ads_sku_sales_analysis;
CREATE TABLE IF NOT EXISTS ads_sku_sales_analysis (
                                                      sku_id          STRING    COMMENT 'SKU唯一标识',
                                                      attribute       STRING    COMMENT '商品属性（如颜色/尺寸）',
                                                      product_id      STRING    COMMENT '商品ID',
                                                      product_name    STRING    COMMENT '商品名称',
                                                      order_count     BIGINT    COMMENT '订单数量',
                                                      sales_volume    BIGINT    COMMENT '销售数量',
                                                      sales_amount   DOUBLE    COMMENT  '销售金额',
                                                      sales_ratio    DOUBLE    COMMENT  '销售占比（占该商品总销售额比例）',
                                                      ds             STRING    COMMENT  '统计日期'
)
    COMMENT 'SKU销售分析表'
    location '/2207A/chenming/360KBan/ads/ads_sku_sales_analysis'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );
insert overwrite table ads_sku_sales_analysis
SELECT

    s.sku_id,
    s.attribute,
    p.product_id,
    p.product_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.quantity) AS sales_volume,
    SUM(o.amount) AS sales_amount,
    SUM(o.amount) / SUM(SUM(o.amount)) OVER (PARTITION BY p.product_id) AS sales_ratio,
        o.ds
FROM
    ods_product_sku s
        JOIN
    ods_product_base p ON s.product_id = p.product_id AND p.ds = '2023-10-01'
        LEFT JOIN
    ods_order_detail o ON s.sku_id = o.sku_id AND o.ds = '2023-10-01'
WHERE
        s.ds = '2023-10-01' and o.ds ='2023-10-01'
GROUP BY
    s.sku_id, s.attribute, p.product_id, p.product_name,o.ds;



-- 价格带分析
drop table ads_product_price_band_analysis;
CREATE TABLE IF NOT EXISTS ads_product_price_band_analysis (
                                                               product_id          STRING    COMMENT '商品ID',
                                                               product_name        STRING    COMMENT '商品名称',
                                                               price              DOUBLE    COMMENT '商品价格',
                                                               price_band         STRING    COMMENT '价格带(0-100/300-600/600+)',
                                                               order_count        BIGINT    COMMENT '订单数量',
                                                               gmv               DOUBLE    COMMENT '成交总额',
                                                               avg_selling_price DOUBLE    COMMENT '实际平均售价',
                                                               ds                 STRING    COMMENT '统计日期'
)
    COMMENT '商品价格带分析表（价格带区间：0-100/300-600/600+）'
    location '/2207A/chenming/360KBan/ads/ads_product_price_band_analysis'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );


INSERT OVERWRITE TABLE ads_product_price_band_analysis
SELECT
    p.product_id,
    p.product_name,
    p.price,
    CASE
        WHEN p.price <= 300 THEN '0-300'  -- 修正为0-300以匹配您的需求
        WHEN p.price > 300 AND p.price <= 600 THEN '300-600'
        ELSE '600+'
        END AS price_band,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.amount) AS gmv,
    AVG(o.amount / NULLIF(o.quantity, 0)) AS avg_selling_price,  -- 添加NULLIF避免除零错误
    o.ds
FROM
    ods_product_base p
        LEFT JOIN
    ods_order_detail o ON p.product_id = o.product_id AND o.ds = '2023-10-01'
WHERE
        p.ds = '2023-10-01' and o.ds = '2023-10-01'
GROUP BY
    p.product_id, p.product_name, p.price,
    CASE
        WHEN p.price <= 300 THEN '0-300'  -- 修正为0-300以匹配您的需求
        WHEN p.price > 300 AND p.price <= 600 THEN '300-600'
        ELSE '600+'
        END,o.ds;



-- 价格变动影响分析
drop  table ads_price_change_impact;
CREATE TABLE IF NOT EXISTS ads_price_change_impact (
                                                       product_id          STRING   COMMENT '商品ID',
                                                       product_name        STRING   COMMENT '商品名称',
                                                       old_price          DOUBLE   COMMENT '变动前价格',
                                                       new_price          DOUBLE   COMMENT '变动后价格',
                                                       price_change_rate  DOUBLE   COMMENT '价格变动率（(新价-旧价)/旧价）',
                                                       ds   string

)
    COMMENT '商品价格变动影响分析表（对比调价前后销售表现）'
    location '/2207A/chenming/360KBan/ads/ads_price_change_impact'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );
insert overwrite  table ads_price_change_impact
SELECT
    p.product_id,
    p.product_name,
    pc.old_price,
    pc.new_price,
    (pc.new_price - pc.old_price) / pc.old_price AS price_change_rate,
    pc.ds
FROM
    ods_price_change_log pc
        JOIN
    ods_product_base p ON pc.product_id = p.product_id AND p.ds = '2023-10-01'
        LEFT JOIN
    ods_order_detail o ON pc.product_id = o.product_id AND o.ds = '2023-10-01'
WHERE
        pc.ds = '2023-10-01' and  o.ds = '2023-10-01'
GROUP BY
    p.product_id, p.product_name, pc.old_price, pc.new_price,
    (pc.new_price - pc.old_price) / pc.old_price ,pc.ds;



-- 流量渠道分析
drop table ads_product_channel_performance;
CREATE TABLE IF NOT EXISTS ads_product_channel_performance (
                                                               product_id       STRING    COMMENT '商品ID',
                                                               product_name     STRING    COMMENT '商品名称',
                                                               channel         STRING    COMMENT '流量渠道（搜索/推荐/直播）',
                                                               pv              BIGINT    COMMENT '页面浏览量',
                                                               uv              BIGINT    COMMENT '独立访客数',
                                                               ds              STRING    COMMENT '统计日期'
)
    COMMENT '商品分渠道效果分析表'
    location '/2207A/chenming/360KBan/ads/ads_product_channel_performance'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );


insert overwrite table ads_product_channel_performance
SELECT
    p.product_id,
    p.product_name,
    u.channel,
    COUNT(DISTINCT u.log_id) AS pv,
    COUNT(DISTINCT u.user_id) AS uv,
    u.ds
FROM
    ods_product_base p
        LEFT JOIN
    ods_user_behavior_log u ON p.product_id = u.product_id AND u.ds = '2023-10-01'
        LEFT JOIN
    ods_order_detail o ON p.product_id = o.product_id AND o.ds = '2023-10-01'
        AND o.user_id = u.user_id
WHERE
        p.ds = '2023-10-01' AND
        u.behavior_type = 'click'
GROUP BY
    p.product_id, p.product_name, u.channel,u.ds;





--标题优化
CREATE external TABLE IF NOT EXISTS ads_product_title_optimization (
  product_id         STRING    COMMENT '商品ID',
  product_name       STRING    COMMENT '商品名称',
  title_root_word    STRING    COMMENT '标题词根',
  search_term_type   STRING    COMMENT '搜索词类型（品类词/长尾词/修饰词）',
  click_user_count   BIGINT    COMMENT '引流人数',
  click_pv           BIGINT    COMMENT '点击量',
  order_count        BIGINT    COMMENT '产生订单数',
  conversion_rate    DOUBLE    COMMENT '转化率',
  avg_order_value    DOUBLE    COMMENT '客单价',
  ds                 STRING    COMMENT '统计日期'
)
COMMENT '商品标题优化分析表（词根引流效果分析）'
location '/2207A/chenming/360KBan/ads/ads_product_title_optimization'
TBLPROPERTIES (
  "orc.compress"="SNAPPY",
  "transactional"="false"
);


-- 建表说明：
-- 1. 包含标题词根拆分分析
-- 2. 区分搜索词类型（需配合业务定义）
-- 3. 关键指标：转化率=订单数/点击用户数
-- 4. 存储按天统计的标题优化效果

-- 数据生成逻辑（需根据实际分词逻辑调整）
insert overwrite table ads_product_title_optimization
SELECT
    p.product_id,
    p.product_name,
    split(p.product_name, ' ')[0] AS title_root_word,  -- 假设第一个词为核心词根
    CASE
        WHEN regexp_extract(p.product_name, '.*(羽绒服|大衣|裤子).*', 1) IS NOT NULL THEN '品类词'
        WHEN regexp_extract(p.product_name, '.*(加厚|新款|冬季).*', 1) IS NOT NULL THEN '修饰词'
        ELSE '长尾词'
        END AS search_term_type,
    COUNT(DISTINCT u.user_id) AS click_user_count,
    COUNT(DISTINCT u.log_id) AS click_pv,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT o.order_id) / NULLIF(COUNT(DISTINCT u.user_id), 0) AS conversion_rate,
    AVG(o.amount) AS avg_order_value,
    u.ds
FROM
    ods_product_base p
        LEFT JOIN
    ods_user_behavior_log u
    ON p.product_id = u.product_id
        AND u.ds = '2023-10-01'
        AND u.behavior_type = 'click'
        LEFT JOIN
    ods_order_detail o
    ON p.product_id = o.product_id
        AND o.ds = '2023-10-01'
        AND o.user_id = u.user_id
WHERE
        p.ds = '2023-10-01'
GROUP BY
    p.product_id, p.product_name,
    split(p.product_name, ' ')[0],
    CASE
        WHEN regexp_extract(p.product_name, '.*(羽绒服|大衣|裤子).*', 1) IS NOT NULL THEN '品类词'
        WHEN regexp_extract(p.product_name, '.*(加厚|新款|冬季).*', 1) IS NOT NULL THEN '修饰词'
        ELSE '长尾词'
        END,
    u.ds;







-- 商品关联分析
drop table if exists ads_product_behavior_analysis;
CREATE TABLE IF NOT EXISTS ads_product_behavior_analysis (
                                                             product_id       STRING    COMMENT '商品ID',
                                                             product_name     STRING    COMMENT '商品名称',
                                                             behavior_type    STRING    COMMENT '用户行为类型（click/view/cart/pay）',
                                                             user_count       BIGINT    COMMENT '触达用户数',
                                                             order_count      BIGINT    COMMENT '产生订单数',
                                                             gmv             DOUBLE    COMMENT '成交总额',
                                                             avg_order_value DOUBLE    COMMENT '客单价',
                                                             ds               STRING
)
    COMMENT '商品用户行为分析表（按行为类型聚合）'
    LOCATION '/2207A/chenming/360KBan/ads/ads_product_behavior_analysis'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );
-- 假设这里的行为类型为 pay，因为是订单相关分析
-- 这里 user_count、order_count、gmv 和 avg_order_value 暂时设置为 0，你可以根据实际情况修改

insert  overwrite table ads_product_behavior_analysis
SELECT
    p.product_id,
    p.product_name,
    u.behavior_type,
    COUNT(DISTINCT u.user_id) AS user_count,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.amount) AS gmv,
    AVG(o.amount) AS avg_order_value,
    p.ds
FROM
    ods_product_base p
        LEFT JOIN
    ods_user_behavior_log u ON p.product_id = u.product_id AND u.ds = '2023-10-01'
        LEFT JOIN
    ods_order_detail o ON p.product_id = o.product_id AND o.ds = '2023-10-01'
        AND o.user_id = u.user_id
WHERE
        p.ds = '2023-10-01'
GROUP BY
    p.product_id, p.product_name, u.behavior_type,p.ds;




-- 评价分析\
drop table ads_product_review;
CREATE TABLE IF NOT EXISTS ads_product_review (
                                                  product_id STRING COMMENT '产品 ID',
                                                  product_name STRING COMMENT '产品名称',
                                                  avg_score DOUBLE COMMENT '平均评分',
                                                  positive_reviews INT COMMENT '正面评价数量',
                                                  negative_reviews INT COMMENT '负面评价数量',
                                                  total_reviews INT COMMENT '总评价数量',
                                                  reviewer_count INT COMMENT '评价用户数量',
                                                  sample_reviews ARRAY<STRING> COMMENT '评价内容样本（前 50 字符）',
                                                  ds string
)
    COMMENT 'ADS 层产品评价汇总表'
    LOCATION '/2207A/chenming/360KBan/ads/ads_product_review'
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "transactional"="false"
                  );

insert overwrite  table ads_product_review
SELECT
    r.product_id,
    p.product_name,
    AVG(r.score) AS avg_score,
    COUNT(CASE WHEN r.score >= 4 THEN 1 END) AS positive_reviews,
    COUNT(CASE WHEN r.score <= 2 THEN 1 END) AS negative_reviews,
    COUNT(*) AS total_reviews,
    COUNT(DISTINCT r.user_id) AS reviewer_count,
    COLLECT_LIST(SUBSTR(CAST(r.content AS STRING), 1, 50)) AS sample_reviews,
    r.ds
FROM
    ods_product_review r
        JOIN
    ods_product_base p ON r.product_id = p.product_id AND p.ds = '2023-10-01'
WHERE
        r.ds = '2023-10-01'
GROUP BY
    r.product_id, p.product_name,r.ds;




-- 删除表
drop table if exists dwt_product_360_view;

-- 创建商品360度综合视图表（ADS层）
CREATE EXTERNAL TABLE IF NOT EXISTS dwt_product_360_view (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '类目ID',
    price DOUBLE COMMENT '商品价格',
    create_time TIMESTAMP COMMENT '上架时间',
    total_order_count BIGINT COMMENT '总订单数',
    total_sales_volume BIGINT COMMENT '总销量',
    total_gmv DOUBLE COMMENT '总成交额',
    buyer_count BIGINT COMMENT '购买人数',
    avg_order_value DOUBLE COMMENT '客单价',
    conversion_rate DOUBLE COMMENT '转化率',
    sku_count INT COMMENT 'SKU数量',
    best_selling_sku STRING COMMENT '畅销SKU',
    sales_ratio_max DOUBLE COMMENT '最高销售占比',
    price_band STRING COMMENT '价格带',
    avg_selling_price DOUBLE COMMENT '实际平均售价',
    price_change_count INT COMMENT '价格变动次数',
    total_uv BIGINT COMMENT '独立访客数',
    title_root_word STRING COMMENT '标题核心词根',
    search_term_type STRING COMMENT '主要搜索词类型',
    cart_user_count BIGINT COMMENT '加购用户数',
    ds STRING COMMENT '统计日期'
)
COMMENT '商品360度综合分析视图'
LOCATION '/2207A/chenming/360KBan/ads/ads_product_360_view'
TBLPROPERTIES (
    "orc.compress" = "SNAPPY",
    "transactional" = "false"
);

-- 使用CTE和INSERT OVERWRITE插入数据
WITH combined_data AS (
    SELECT
        base.product_id,
        base.product_name,
        base.category_id,
        base.price,
        base.create_time,
        sales_core.order_count AS total_order_count,
        sales_core.sales_volume AS total_sales_volume,
        sales_core.gmv AS total_gmv,
        sales_core.buyer_count,
        sales_core.avg_order_value,
        sales_core.conversion_rate,
        sku.sku_id,
        sku.sales_amount,
        sku.sales_ratio,
        price_band.price_band,
        price_band.avg_selling_price,
        price_change.product_id AS price_change_product_id,
        channel_performance.uv AS total_uv,
        channel_performance.channel,
        title_optimization.title_root_word,
        title_optimization.search_term_type,
        behavior_analysis.behavior_type,
        behavior_analysis.user_count,
        activity.activity_id,
        activity.action_type,
        base.ds
    FROM
        ods_product_base base
            -- 连接销售核心指标表
            LEFT JOIN ads_product_sales_core sales_core
                      ON base.product_id = sales_core.product_id AND base.ds = sales_core.ds
            -- 连接SKU销售分析表
            LEFT JOIN ads_sku_sales_analysis sku
                      ON base.product_id = sku.product_id AND base.ds = sku.ds
            -- 连接价格带分析表
            LEFT JOIN ads_product_price_band_analysis price_band
                      ON base.product_id = price_band.product_id AND base.ds = price_band.ds
            -- 连接价格变动影响分析表
            LEFT JOIN ads_price_change_impact price_change
                      ON base.product_id = price_change.product_id AND base.ds = price_change.ds
            -- 连接流量渠道分析表
            LEFT JOIN ads_product_channel_performance channel_performance
                      ON base.product_id = channel_performance.product_id AND base.ds = channel_performance.ds
            -- 连接标题优化分析表
            LEFT JOIN ads_product_title_optimization title_optimization
                      ON base.product_id = title_optimization.product_id AND base.ds = title_optimization.ds
            -- 连接商品用户行为分析表
            LEFT JOIN ads_product_behavior_analysis behavior_analysis
                      ON base.product_id = behavior_analysis.product_id AND base.ds = behavior_analysis.ds
            -- 连接商品评价分析表
            LEFT JOIN ads_product_review review
                      ON base.product_id = review.product_id AND base.ds = review.ds
            -- 连接运营活动表
            LEFT JOIN ods_operation_activity activity
                      ON base.product_id = activity.product_id AND base.ds = activity.ds
                          AND activity.end_time > CURRENT_TIMESTAMP
    WHERE
            base.ds = '2023-10-01'
)
INSERT OVERWRITE TABLE dwt_product_360_view
SELECT
    product_id,
    product_name,
    category_id,
    price,
    create_time,
    total_order_count,
    total_sales_volume,
    total_gmv,
    buyer_count,
    avg_order_value,
    conversion_rate,
    COUNT(DISTINCT sku_id) AS sku_count,
    FIRST_VALUE(sku_id) OVER (PARTITION BY product_id ORDER BY sales_amount DESC) AS best_selling_sku,
        MAX(sales_ratio) AS sales_ratio_max,
    price_band,
    avg_selling_price,
    COUNT(DISTINCT price_change_product_id) AS price_change_count,
    total_uv,
    title_root_word,
    search_term_type,
    SUM(CASE WHEN behavior_type = 'cart' THEN user_count END) AS cart_user_count,
    ds
FROM
    combined_data
GROUP BY
    product_id,
    product_name,
    category_id,
    price,
    create_time,
    total_order_count,
    total_sales_volume,
    total_gmv,
    buyer_count,
    avg_order_value,
    conversion_rate,
    price_band,
    avg_selling_price,
    title_root_word,
    search_term_type,
    ds,
    sales_amount,
    sku_id,
    total_uv;