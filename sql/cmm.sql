
drop table if exists ods_order_info;
create table ods_order_info (
                                `id` string COMMENT '订单编号',
                                `total_amount` decimal(10,2) COMMENT '订单金额',
                                `order_status` string COMMENT '订单状态',
                                `user_id` string COMMENT '用户id' ,
                                `payment_way` string COMMENT '支付方式',
                                `out_trade_no` string COMMENT '支付流水号',
                                `create_time` string COMMENT '创建时间',
                                `operate_time` string COMMENT '操作时间'
) COMMENT '订单表'
PARTITIONED BY ( `dt` string)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/ods/ods_order_info/'
TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );


drop table if exists ods_order_detail;
create table ods_order_detail(
                                 `id` string COMMENT '订单编号',
                                 `order_id` string  COMMENT '订单号',
                                 `user_id` string COMMENT '用户id' ,
                                 `sku_id` string COMMENT '商品id',
                                 `sku_name` string COMMENT '商品名称',
                                 `order_price` string COMMENT '下单价格',
                                 `sku_num` string COMMENT '商品数量',
                                 `create_time` string COMMENT '创建时间'
) COMMENT '订单明细表'
    PARTITIONED BY ( `dt` string)
    row format delimited  fields terminated by '\t'
    location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/ods/ods_order_detail/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );





drop table if exists ods_sku_info;
create table ods_sku_info(
                             `id` string COMMENT 'skuId',
                             `spu_id` string  COMMENT 'spuid',
                             `price` decimal(10,2) COMMENT '价格' ,
                             `sku_name` string COMMENT '商品名称',
                             `sku_desc` string COMMENT '商品描述',
                             `weight` string COMMENT '重量',
                             `tm_id` string COMMENT '品牌id',
                             `category3_id` string COMMENT '品类id',
                             `create_time` string COMMENT '创建时间'
) COMMENT '商品表'
    PARTITIONED BY ( `dt` string)
    row format delimited  fields terminated by '\t'
    location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/ods/ods_sku_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


drop table if exists ods_user_info;
create table ods_user_info(
                              `id` string COMMENT '用户id',
                              `name`  string COMMENT '姓名',
                              `birthday` string COMMENT '生日' ,
                              `gender` string COMMENT '性别',
                              `email` string COMMENT '邮箱',
                              `user_level` string COMMENT '用户等级',
                              `create_time` string COMMENT '创建时间'
) COMMENT '用户信息'
    PARTITIONED BY ( `dt` string)
    row format delimited  fields terminated by '\t'
    location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/ods/ods_user_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


drop table if exists ods_base_category1;
create table ods_base_category1(
                                   `id` string COMMENT 'id',
                                   `name`  string COMMENT '名称'
) COMMENT '商品一级分类'
    PARTITIONED BY ( `dt` string)
    row format delimited  fields terminated by '\t'
    location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/ods/ods_base_category1/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


drop table if exists ods_base_category2;
create external table ods_base_category2(
                                            `id` string COMMENT ' id',
                                            `name`  string COMMENT '名称',
                                            category1_id string COMMENT '一级品类id'
) COMMENT '商品二级分类'
    PARTITIONED BY ( `dt` string)
    row format delimited  fields terminated by '\t'
    location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/ods/ods_base_category2/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

drop table if exists ods_base_category3;
create table ods_base_category3(
                                   `id` string COMMENT ' id',
                                   `name`  string COMMENT '名称',
                                   category2_id string COMMENT '二级品类id'
) COMMENT '商品三级分类'
    PARTITIONED BY ( `dt` string)
    row format delimited  fields terminated by '\t'
    location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/ods/ods_base_category3/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

drop table if exists `ods_payment_info`;
create table  `ods_payment_info`(
                                    `id`  bigint COMMENT '编号',
                                    `out_trade_no`   string COMMENT '对外业务编号',
                                    `order_id`  string COMMENT '订单编号',
                                    `user_id`  string COMMENT '用户编号',
                                    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
                                    `total_amount`  decimal(16,2) COMMENT '支付金额',
                                    `subject`  string COMMENT '交易内容',
                                    `payment_type` string COMMENT '支付类型',
                                    `payment_time`  string COMMENT '支付时间'
)  COMMENT '支付流水表'
    PARTITIONED BY ( `dt` string)
    row format delimited  fields terminated by '\t'
    location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/ods/ods_payment_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


drop table if exists `ods_comment_info`;
create table  `ods_comment_info`(
                                    `id`  bigint COMMENT '编号',
                                    `user_id`   string COMMENT '用户id',
                                    `sku_id`  string COMMENT '商品id',
                                    `spu_id`  string COMMENT '品牌id',
                                    `order_id` string COMMENT '订单编号',
                                    `appraise`  decimal(16,2) COMMENT '评价',
                                    `comment_txt`  string COMMENT '评价类容',
                                    `create_time` string COMMENT '创建时间',
                                    `operate_time`  string COMMENT '修改时间'
)  COMMENT '支付流水表'
    PARTITIONED BY ( `dt` string)
    row format delimited  fields terminated by '\t'
    location '/warehouse/db_gmall/ods/ods_comment_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


load data inpath '/2207A/chenming/order_info/2025-03-23/order_info__d8148296_2396_4f38_9b85_556475eae74c.gz'  OVERWRITE into table ods_order_info partition(dt='2025-03-23');
load data inpath '/2207A/chenming/order_detail/2025-03-23'  OVERWRITE into table ods_order_detail partition(dt='2025-03-23');
load data inpath '/2207A/chenming/sku_info/2025-03-23'  OVERWRITE into table ods_sku_info partition(dt='2025-03-23');
load data inpath '/2207A/chenming/user_info/2025-03-23' OVERWRITE into table ods_user_info partition(dt='2025-03-23');
load data inpath '/2207A/chenming/payment_info/2025-03-23' OVERWRITE into table ods_payment_info partition(dt='2025-03-23');
load data inpath '/2207A/chenming/base_category1/2025-03-23' OVERWRITE into table ods_base_category1 partition(dt='2025-03-23');
load data inpath '/2207A/chenming/base_category2/2025-03-23' OVERWRITE into table ods_base_category2 partition(dt='2025-03-23');
load data inpath '/2207A/chenming/base_category3/2025-03-23' OVERWRITE into table ods_base_category3 partition(dt='2025-03-23');
load data inpath '/2207A/chenming/order_info/2025-03-23' OVERWRITE into table ods_comment_info partition(dt='2025-03-23');



-- 3.4.1 创建订单表
drop table if exists dwd_order_info;
create external table dwd_order_info (
  `id` string COMMENT '',
   `total_amount` decimal(10,2) COMMENT '',
   `order_status` string COMMENT ' 1 2  3  4  5',
  `user_id` string COMMENT 'id' ,
 `payment_way` string COMMENT '',
 `out_trade_no` string COMMENT '',
 `create_time` string COMMENT '',
 `operate_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/dwd/dwd_order_info/'
tblproperties ("parquet.compression"="snappy")
;




drop table if exists dwd_order_detail;
create external table dwd_order_detail(
   `id` string COMMENT '',
   `order_id` decimal(10,2) COMMENT '',
  `user_id` string COMMENT 'id' ,
 `sku_id` string COMMENT 'id',
 `sku_name` string COMMENT '',
 `order_price` string COMMENT '',
 `sku_num` string COMMENT '',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/dwd/dwd_order_detail/'
tblproperties ("parquet.compression"="snappy")
;


drop table if exists dwd_user_info;
create external table dwd_user_info(
   `id` string COMMENT 'id',
   `name`  string COMMENT '',
  `birthday` string COMMENT '' ,
 `gender` string COMMENT '',
 `email` string COMMENT '',
 `user_level` string COMMENT '',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/dwd/dwd_user_info/'
tblproperties ("parquet.compression"="snappy")
;
-- 3.4.4 创建支付流水表

drop table if exists `dwd_payment_info`;
create external  table  `dwd_payment_info`(
`id`  bigint COMMENT '',
`out_trade_no`  string COMMENT '',
`order_id`  string COMMENT '',
`user_id`  string COMMENT '',
`alipay_trade_no` string COMMENT '',
`total_amount`  decimal(16,2) COMMENT '',
`subject`  string COMMENT '',
`payment_type` string COMMENT '',
`payment_time`  string COMMENT ''
 )  COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/dwd/dwd_payment_info/'
tblproperties ("parquet.compression"="snappy")
;


drop table if exists dwd_sku_info;
create external table dwd_sku_info(
   `id` string COMMENT 'skuId',
   `spu_id` string COMMENT 'spuid',
  `price` decimal(10,2) COMMENT '' ,
 `sku_name` string COMMENT '',
 `sku_desc` string COMMENT '',
 `weight` string COMMENT '',
 `tm_id` string COMMENT 'id',
 `category3_id` string COMMENT '1id',
 `category2_id` string COMMENT '2id',
 `category1_id` string COMMENT '3id',
 `category3_name` string COMMENT '3',
 `category2_name` string COMMENT '2',
 `category1_name` string COMMENT '1',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="snappy")
;


drop table if exists dwd_comment_info;
create external table dwd_comment_info(
    id           bigint comment '??',
    user_id      string comment '??id',
    sku_id       string comment '??id',
    spu_id       string comment '??id',
    order_id     string comment '????',
    appraise     decimal(16, 2) comment '??',
    comment_txt  string comment '????',
    create_time  string comment '????',
    operate_time string comment '????'
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/dwd/dwd_comment_info/'
tblproperties ("parquet.compression"="snappy")
;



drop table if exists dwd_comment_log;
CREATE EXTERNAL TABLE dwd_comment_log(
`mid_id` string,
`user_id` string,
`version_code` string,
`version_name` string,
`lang` string,
`source` string,
`os` string,
`area` string,
`model` string,
`brand` string,
`sdk_version` string,
`gmail` string,
`height_width` string,
`app_time` string,
`network` string,
`lng` string,
`lat` string,
`comment_id` int,
`userid` int,
`p_comment_id` int,
`content` string,
`addtime` string,
`other_id` int,
`praise_count` int,
`reply_count` int,
`server_time` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/dwd/dwd_comment_log/'
TBLPROPERTIES('parquet.compression'='snappy');



drop table if exists dwd_sku_info;
create external table dwd_sku_info(
   `id` string COMMENT 'skuId',
   `spu_id` string COMMENT 'spuid',
  `price` decimal(10,2) COMMENT '' ,
 `sku_name` string COMMENT '',
 `sku_desc` string COMMENT '',
 `weight` string COMMENT '',
 `tm_id` string COMMENT 'id',
 `category3_id` string COMMENT '1id',
 `category2_id` string COMMENT '2id',
 `category1_id` string COMMENT '3id',
 `category3_name` string COMMENT '3',
 `category2_name` string COMMENT '2',
 `category1_name` string COMMENT '1',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="snappy")
;



set hive.exec.dynamic.partition.mode=nonstrict;
insert  overwrite table   dwd_order_info partition(dt )
select  * from ods_order_info
where dt='2025-03-23'  and id is not null;


insert  overwrite table   dwd_order_detail partition(dt)
select  * from ods_order_detail
where dt='2025-03-23'  and id is not null;

insert  overwrite table  dwd_sku_info partition(dt)
select  * from ods_sku_info
where dt='2025-03-23'  and id is not null ;


insert  overwrite table   dwd_payment_info partition(dt)
select  * from ods_payment_info
where dt='2025-03-23'  and id is not null   ;

insert  overwrite table   dwd_user_info partition(dt)
select  * from ods_user_info
where dt='2025-03-23'  and id is not null;



-- 6.2.1 复购率
drop table if exists dws_sale_detail_daycount;
create external table  dws_sale_detail_daycount
(  user_id  string  comment '用户 id',
 sku_id  string comment '商品 Id',
 user_gender  string comment '用户性别',
 user_age string  comment '用户年龄',
 user_level string comment '用户等级',
 order_price decimal(10,2) comment '订单价格',
 sku_name string  comment '商品名称',
 sku_tm_id string  comment '品牌id',
 sku_category3_id string comment '商品三级品类id',
 sku_category2_id string comment '商品二级品类id',
 sku_category1_id string comment '商品一级品类id',
sku_category3_name string comment '商品三级品类名称',
 sku_category2_name string comment '商品二级品类名称',
sku_category1_name string comment '商品一级品类名称',
 spu_id  string comment '商品 spu',
 sku_num  int comment '购买个数',
 order_count string comment '当日下单单数',
 order_amount string comment '当日下单金额'
) COMMENT '用户购买商品明细表'
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/dws/dws_user_sale_detail_daycount/'
tblproperties ("parquet.compression"="snappy");





with
    tmp_detail as
        (
            select
                user_id,
                sku_id,
                sum(sku_num) sku_num ,
                count(*) order_count ,
                sum(od.order_price*sku_num)  order_amount
            from ods_order_detail od
            where od.dt='2025-03-23' and user_id is not null
            group by user_id, sku_id
        )
insert overwrite table  dws_sale_detail_daycount partition(dt='2025-03-23')
select
    tmp_detail.user_id,
    tmp_detail.sku_id,
    u.gender,
    months_between('2025-03-23', u.birthday)/12  age,
    u.user_level,
    price,
    sku_name,
    tm_id,
    category3_id ,
    category2_id ,
    category1_id ,
    category3_name ,
    category2_name ,
    category1_name ,
    spu_id,
    tmp_detail.sku_num,
    tmp_detail.order_count,
    tmp_detail.order_amount
from tmp_detail
         left join dwd_user_info u on u.id=tmp_detail.user_id  and u.dt='2025-03-23'
         left join dwd_sku_info s on tmp_detail.sku_id =s.id  and s.dt='2025-03-23'
;


select * from dws_sale_detail_daycount where dt='2025-03-11' limit 2;



-- 5.2 ADS层之新增用户占日活跃用户比率
drop table if exists ads_user_convert_day;
create   table ads_user_convert_day(
                                       `dt` string COMMENT '统计日期',
                                       `uv_m_count`  bigint COMMENT '当日活跃设备',
                                       `new_m_count`  bigint COMMENT '当日新增设备',
                                       `new_m_ratio`   decimal(10,2) COMMENT '当日新增占日活的比率'
) COMMENT '每日活跃用户数量'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_convert_day/'
;

-- 日活跃用户比率
--这个需要第一周的 新增设备指标 ads_new_mid_count
insert into table ads_user_convert_day
select
    '2025-03-10',
    sum( uc.dc) sum_dc,
    sum( uc.nmc) sum_nmc,
    cast(sum( uc.nmc)/sum( uc.dc)*100 as decimal(10,2))  new_m_ratio
from
    (
        select
            day_count dc,
            0 nmc
        from ads_uv_count
        where  dt='2025-02-25'

        union all
        select
            0 dc,
            new_mid_count nmc
        from ads_new_mid_count
        where create_date='2025-02-25'
    )uc;


select * from ads_user_convert_day;


-- 5.3 ADS层之用户行为漏斗分析

drop table if exists ads_user_action_convert_day;
create   table ads_user_action_convert_day(
                                              `dt` string COMMENT '统计日期',
                                              `total_visitor_m_count`  bigint COMMENT '总访问人数',
                                              `order_u_count` bigint     COMMENT '下单人数',
                                              `visitor2order_convert_ratio`  decimal(10,2) COMMENT '访问到下单转化率',
                                              `payment_u_count` bigint     COMMENT '支付人数',
                                              `order2payment_convert_ratio` decimal(10,2) COMMENT '下单到支付的转化率'
) COMMENT '每日用户行为转化率统计'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_convert_day/'
;

insert into table ads_user_action_convert_day
select
    '2025-03-10',
    uv.day_count,
    ua.order_count,
    cast(ua.order_count/uv.day_count*100 as  decimal(10,2)) visitor2order_convert_ratio,
    ua.payment_count,
    cast(ua.payment_count/ua.order_count*100 as  decimal(10,2)) order2payment_convert_ratio
from
    (
        select
            sum(if(order_count>0,1,0)) order_count,
            sum(if(payment_count>0,1,0)) payment_count
        from dws_user_action
        where  dt='2025-03-10'
    )ua, ads_uv_count  uv
where  uv.dt='2025-02-25'
;


select * from ads_user_action_convert_day;


-- 6.3 品牌复购率分析

drop  table ads_sale_tm_category1_stat_mn;
create  table ads_sale_tm_category1_stat_mn
(
    tm_id string comment '品牌id ' ,
    category1_id string comment '1级品类id ',
    category1_name string comment '1级品类名称 ',
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(10,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(10,2)  comment  '多次复购率' ,
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期'
)   COMMENT '复购率统计'
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_chenming.db/hivetablename/ads/ads_sale_tm_category1_stat_mn/'
;



insert into table ads_sale_tm_category1_stat_mn
select
    mn.sku_tm_id,
    mn.sku_category1_id,
    mn.sku_category1_name,
    sum(if(mn.order_count>=1,1,0)) buycount,
    sum(if(mn.order_count>=2,1,0)) buyTwiceLast,
    sum(if(mn.order_count>=2,1,0))/sum( if(mn.order_count>=1,1,0)) buyTwiceLastRatio,
    sum(if(mn.order_count>3,1,0))  buy3timeLast  ,
    sum(if(mn.order_count>=3,1,0))/sum( if(mn.order_count>=1,1,0)) buy3timeLastRatio ,
    date_format('2025-03-23' ,'yyyy-MM') stat_mn,
    '2025-03-23' stat_date
from
    (
        select od.sku_tm_id,
               od.sku_category1_id,
               od.sku_category1_name,
               user_id ,
               sum(order_count) order_count
        from  dws_sale_detail_daycount  od
        where
                date_format(dt,'yyyy-MM')<=date_format('2025-03-23' ,'yyyy-MM')
        group by
            od.sku_tm_id, od.sku_category1_id, user_id, od.sku_category1_name
    ) mn
group by mn.sku_tm_id, mn.sku_category1_id, mn.sku_category1_name
;



