create database dev_realtime_tms01_chenming;
use dev_realtime_tms01_chenming;


drop table  ods_order_info;
create  table ods_order_info
(
    id                   string  comment 'id',
    order_no             string             not null comment '订单号',
    status               string             comment '订单状态',
    collect_type         string  comment '取件类型，1为网点自寄，2为上门取件',
    user_id              string                  comment '客户id',
    receiver_complex_id  string                   comment '收件人小区id',
    receiver_province_id string              comment '收件人省份id',
    receiver_city_id     string              comment '收件人城市id',
    receiver_district_id string            comment '收件人区县id',
    receiver_address     string            comment '收件人详细地址',
    receiver_name        string             comment '收件人姓名',
    receiver_phone       string           comment '收件人电话',
    receive_location     string              comment '起始点经纬度',
    sender_complex_id    string                 comment '发件人小区id',
    sender_province_id   string            comment '发件人省份id',
    sender_city_id       string              comment '发件人城市id',
    sender_district_id   string              comment '发件人区县id',
    sender_address       string             comment '发件人详细地址',
    sender_name          string             comment '发件人姓名',
    sender_phone         string            comment '发件人电话',
    send_location        string              comment '发件人坐标',
    payment_type         string              comment '支付方式',
    cargo_num            int                      comment '货物个数',
    amount               decimal(32, 2)           comment '金额',
    estimate_arrive_time string                comment '预计到达时间',
    distance             decimal(10, 2)           comment '距离，单位：公里',
    create_time          string              not null comment '创建时间',
    update_time          string               comment '更新时间',
    is_deleted           string   comment '删除标记（0:不可用 1:可用）'
) COMMENT'订单表'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/ods/ods_order_info'
tblproperties ("parquet.compression"="snappy" );


drop table  ods_order_cargo;
create table ods_order_cargo
(
    id            bigint ,
    order_id      string         comment '订单id',
    cargo_type    string comment '货物类型id',
    volume_length int                     comment '长cm',
    volume_width  int                     comment '宽cm',
    volume_height int                      comment '高cm',
    weight        decimal(16, 2)          comment '重量 kg',
    remark        string                 comment '备注',
    create_time   string             comment '创建时间',
    update_time   string            comment '更新时间',
    is_deleted    string comment '删除标记（0:不可用 1:可用）'
)COMMENT'订单明细表'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/ods/ods_order_cargo'
tblproperties ("parquet.compression"="snappy" );

drop table  ods_base_dic;
create table ods_base_dic
(
    id          string        comment 'id',
    parent_id   string       comment '上级id',
    name        string  comment '名称',
    dict_code   string    comment '编码',
    create_time string     comment '创建时间',
    update_time string      comment '更新时间',
    is_deleted  string      comment '删除标记（0:不可用 1:可用）'
)COMMENT'数字字典'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/ods/ods_base_dic'
tblproperties ("parquet.compression"="snappy" );


drop table  ods_base_complex;
create table ods_base_complex
(
    id            string ,
    complex_name  string     ,
    province_id   string            ,
    city_id       string           ,
    district_id   string             ,
    district_name string    ,
    create_time   string           ,
    update_time   string           ,
    is_deleted    string
)COMMENT''
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/ods/ods_base_complex'
tblproperties ("parquet.compression"="snappy" );


drop table  ods_base_region_info;
create table ods_base_region_info
(
    id          bigint               comment 'id',
    parent_id   string               comment '上级id',
    name        string               comment '名称',
    dict_code   string               comment '编码',
    short_name  string               comment '简称',
    create_time string               comment '创建时间',
    update_time string               comment '更新时间',
    is_deleted  string               comment '删除标记（0:不可用 1:可用）'
)COMMENT'数字字典'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/ods/ods_base_region_info'
tblproperties ("parquet.compression"="snappy" );


drop table  ods_base_organ;
create table ods_base_organ
(
    id            bigint ,
    org_name      string           comment '机构名称',
    org_level     string           comment '行政级别',
    region_id     string           comment '区域id，1级机构为city ,2级机构为district',
    org_parent_id string           comment '上级机构id',
    points        string           comment '多边形经纬度坐标集合',
    create_time   string    comment '创建时间',
    update_time   string    comment '更新时间',
    is_deleted    string      comment '删除标记（0:不可用 1:可用）'
)COMMENT'机构范围表'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/ods/ods_base_organ'
tblproperties ("parquet.compression"="snappy" );


load data inpath '/2207A/chenming/tms/order_info/2025-03-24' overwrite  into table  ods_order_info partition (dt ='2025-03-24');
load data inpath '/2207A/chenming/tms/order_cargo/2025-03-24' overwrite  into table  ods_order_cargo partition (dt ='2025-03-24');
load data inpath '/2207A/chenming/tms/base_region_info/2025-03-24' overwrite  into table  ods_base_region_info partition (dt ='2025-03-24');
load data inpath '/2207A/chenming/tms/base_organ/2025-03-24' overwrite  into table  ods_base_organ partition (dt ='2025-03-24');
load data inpath '/2207A/chenming/tms/base_dic/2025-03-24' overwrite  into table  ods_base_dic partition (dt ='2025-03-24');
load data inpath '/2207A/chenming/tms/base_complex/2025-03-24' overwrite  into table  ods_base_complex partition (dt ='2025-03-24');












drop table  dwd_order_info;
create  table dwd_order_info
(
    id                   string  comment 'id',
    order_no             string             not null comment '订单号',
    status               string             comment '订单状态',
    collect_type         string  comment '取件类型，1为网点自寄，2为上门取件',
    user_id              string                  comment '客户id',
    receiver_complex_id  string                   comment '收件人小区id',
    receiver_province_id string              comment '收件人省份id',
    receiver_city_id     string              comment '收件人城市id',
    receiver_district_id string            comment '收件人区县id',
    receiver_address     string            comment '收件人详细地址',
    receiver_name        string             comment '收件人姓名',
    receiver_phone       string           comment '收件人电话',
    receive_location     string              comment '起始点经纬度',
    sender_complex_id    string                 comment '发件人小区id',
    sender_province_id   string            comment '发件人省份id',
    sender_city_id       string              comment '发件人城市id',
    sender_district_id   string              comment '发件人区县id',
    sender_address       string             comment '发件人详细地址',
    sender_name          string             comment '发件人姓名',
    sender_phone         string            comment '发件人电话',
    send_location        string              comment '发件人坐标',
    payment_type         string              comment '支付方式',
    cargo_num            int                      comment '货物个数',
    amount               decimal(32, 2)           comment '金额',
    estimate_arrive_time string                comment '预计到达时间',
    distance             decimal(10, 2)           comment '距离，单位：公里',
    create_time          string              not null comment '创建时间',
    update_time          string               comment '更新时间',
    is_deleted           string   comment '删除标记（0:不可用 1:可用）'
) COMMENT'订单表'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/dwd/dwd_order_info'
tblproperties ("parquet.compression"="snappy" );



create table dwd_order_cargo
(
    id            bigint ,
    order_id      string         comment '订单id',
    cargo_type    string comment '货物类型id',
    volume_length int                     comment '长cm',
    volume_width  int                     comment '宽cm',
    volume_height int                      comment '高cm',
    weight        decimal(16, 2)          comment '重量 kg',
    remark        string                 comment '备注',
    create_time   string             comment '创建时间',
    update_time   string            comment '更新时间',
    is_deleted    string comment '删除标记（0:不可用 1:可用）'
)COMMENT'订单明细表'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/dwd/dwd_order_cargo'
tblproperties ("parquet.compression"="snappy" );

drop table  dwd_base_dic;
create table dwd_base_dic
(
    id          string        comment 'id',
    parent_id   string       comment '上级id',
    name        string  comment '名称',
    dict_code   string    comment '编码',
    create_time string     comment '创建时间',
    update_time string      comment '更新时间',
    is_deleted  string      comment '删除标记（0:不可用 1:可用）'
)COMMENT'数字字典'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/dwd/dwd_base_dic'
tblproperties ("parquet.compression"="snappy" );


drop table  dwd_base_complex;
create table dwd_base_complex
(
    id            string ,
    complex_name  string     ,
    province_id   string            ,
    city_id       string           ,
    district_id   string             ,
    district_name string    ,
    create_time   string           ,
    update_time   string           ,
    is_deleted    string
)COMMENT''
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/dwd/dwd_base_complex'
tblproperties ("parquet.compression"="snappy" );


drop table  dwd_base_region_info;
create table dwd_base_region_info
(
    id          bigint               comment 'id',
    parent_id   string               comment '上级id',
    name        string               comment '名称',
    dict_code   string               comment '编码',
    short_name  string               comment '简称',
    create_time string               comment '创建时间',
    update_time string               comment '更新时间',
    is_deleted  string               comment '删除标记（0:不可用 1:可用）'
)COMMENT'数字字典'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/dwd/dwd_base_region_info'
tblproperties ("parquet.compression"="snappy" );


drop table  dwd_base_organ;
create table dwd_base_organ
(
    id            bigint ,
    org_name      string           comment '机构名称',
    org_level     string           comment '行政级别',
    region_id     string           comment '区域id，1级机构为city ,2级机构为district',
    org_parent_id string           comment '上级机构id',
    points        string           comment '多边形经纬度坐标集合',
    create_time   string    comment '创建时间',
    update_time   string    comment '更新时间',
    is_deleted    string      comment '删除标记（0:不可用 1:可用）'
)COMMENT'机构范围表'
    partitioned by (`dt` string)
row format delimited
fields terminated by '\t'
location'/2207A/chenming/tms/dwd/dwd_base_organ'
tblproperties ("parquet.compression"="snappy" );


set hive.exec.dynamic.partition.mode=nonstrict;

insert  overwrite table   dwd_order_info partition(dt)
select  * from ods_order_info
where dt='2025-03-24'  and id is not null;

insert  overwrite table   dwd_order_cargo partition(dt)
select  * from  ods_order_cargo
where dt='2025-03-24'   and id is not null;

insert  overwrite table  dwd_base_region_info partition(dt)
select  * from ods_base_region_info
where dt='2025-03-24'   and id is not null;

insert  overwrite table   dwd_base_organ partition(dt)
select  * from ods_base_organ
where dt='2025-03-24'  and id is not null;

insert  overwrite table   dwd_base_dic partition(dt)
select  * from ods_base_dic
where dt='2025-03-24'  and id is not null;

insert  overwrite table   dwd_base_complex partition(dt)
select  * from ods_base_complex
where dt='2025-03-24'  and id is not null;
--order_cargo  order_info dwd_base_region_info
-- 业务汇总需求：按照【货物类型 + 区域 + 取件类型】粒度，对DWD层事实表进行数据汇总计算下单数和下单金额，其中区域以收件人城市为准。

-- 1）、依据上述需求，分别设计DWS汇总层，
-- 最近1日数据汇总表和最近30日（1月）数据汇总表，编写DDL语句，并执行创建表，其中表设约束如同DWD层表。（6分，每个表3分）

CREATE TABLE dws_cargo_order_daily_sum (

                                           cargo_type        string COMMENT '货物类型',
                                           receiver_city     string COMMENT '收件人城市（区域）',
                                           collect_type      string COMMENT '取件类型',
                                           order_count       bigint COMMENT '下单数',
                                           amount_sum        decimal(32, 2) COMMENT '下单金额总和'
) COMMENT '最近1日数据汇总表'
PARTITIONED BY (`dt` string)
STORED AS PARQUET
LOCATION '/2207A/chenming/tms/dws_cargo_order_daily_sum/'
TBLPROPERTIES (
    "compression"="lzo",
    "transactional"="true"
);

CREATE TABLE dws_cargo_order_monthly_sum (
                                             cargo_type        string COMMENT '货物类型',
                                             receiver_city     string COMMENT '收件人城市（区域）',
                                             collect_type      string COMMENT '取件类型',
                                             order_count       bigint COMMENT '下单数',
                                             amount_sum        decimal(32, 2) COMMENT '下单金额总和'
) COMMENT '最近30日数据汇总表'
PARTITIONED BY (`dt` string)
STORED AS PARQUET
LOCATION '/2207A/chenming/tms/dws_cargo_order_monthly_sum/'
TBLPROPERTIES (
    "compression"="lzo",
    "transactional"="true"
);


INSERT INTO dws_cargo_order_daily_sum PARTITION(dt)
SELECT
    cargo_type,
    receiver_city,
    collect_type,
    COUNT(*) AS order_count,
    SUM(amount) AS amount_sum,
    CURRENT_DATE AS dt
FROM dwd_base_region_info  re LEFT JOIN dwd_order_info oi on re.id =oi.id
                              left join
     WHERE dt = '${current_date}'
GROUP BY cargo_type, receiver_city, collect_type;
-- 2）、编写HiveSQL，完成最近1日数据汇总表数据装载，并查看汇总表结果；（5分）
-- 3）、依据最近1日汇总结果数据，编写SQL语句统计最近30日（1月）数据数据装载；（4分）