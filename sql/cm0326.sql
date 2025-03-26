drop table dws_cargo_order_daily1_sum;
CREATE TABLE dws_cargo_order_daily1_sum (

                                            cargo_type        string COMMENT '货物类型',
                                            receiver_city     string COMMENT '收件人城市（区域）',
                                            collect_type      string COMMENT '取件类型',
                                            order_count       bigint COMMENT '下单数',
                                            amount_sum        decimal(32, 2) COMMENT '下单金额总和'
) COMMENT '最近1日数据汇总表'
PARTITIONED BY (`dt` string)
STORED AS PARQUET
LOCATION '/2207A/chenming/tms/dws/dws_cargo_order_daily_sum/'
TBLPROPERTIES (
    "compression"="lzo"
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
LOCATION '/2207A/chenming/tms/dws/dws_cargo_order_monthly_sum/'
TBLPROPERTIES (
    "compression"="lzo"
);


set hive.exec.dynamic.partition.mode=nonstrict;
insert into dws_cargo_order_daily1_sum partition (dt='2025-03-24')
select
    cargo_type,
    receiver_city,
    collect_type ,
    order_count,
    amount_sum

from (select cargo_type,
             fo.name as receiver_city,
             collect_type,
             count(distinct ia.id) as order_count,
             sum(amount * cargo_num)  amount_sum,
             fo.dt

      from ods_order_info ia
               left join ods_order_cargo c on ia.id = c.order_id
               left join dwd_base_dic cc on c.cargo_type = cc.id
               left join dwd_base_region_info fo on ia.receiver_city_id = fo.id
      where c.dt ='2025-03-24' and cc.dt='2025-03-24' and fo.dt='2025-03-24'
      group by cargo_type,fo.name,collect_type,fo.dt
     )a
where a.dt ='2025-03-24';



set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_cargo_order_daily1_sum  partition (dt)
select
    dic.name,
    region.name,
    collect_type,
    count(distinct info.id) order_count,
    sum(amount*cargo_num) amount_sum,
    dt
from(
        select
            id,
            collect_type,
            receiver_city_id,
            amount,
            cargo_num,
            dt
        from ods_order_info
    ) info left join(
    select
        order_id,
        cargo_type
    from ods_order_cargo
) cargo on info.id=cargo.order_id
           left join(
    select
        id,
        name
    from dwd_base_dic
)dic on dic.id=cargo.cargo_type
           left join(
    select
        id,
        name
    from dwd_base_region_info
)region on region.id=info.receiver_city_id
group by dic.name, region.name, collect_type, dt ;





insert overwrite table dws_cargo_order_monthly_sum  partition (dt="2025-03-24")
select
    cargo_type,
    receiver_city,
    collect_type,
    sum(order_count),
    sum(amount_sum)
from dws_cargo_order_daily1_sum
-- where date_format(dt,'yyyy-MM') = date_format('2025-03-24','yyyy-MM')
where dt>=date_sub('2025-03-24',29) and dt<='2025-03-24'
group by cargo_type,  receiver_city,collect_type;



-- 1）、运单综合统计：统计最近1日、7日、30日总的运单数量和运单金额； （5分）
drop table if exists dws_trade_org_cargo_type_order_1d;
create external table dws_trade_org_cargo_type_order_1d(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`cargo_type` string comment '货物类型',
	`cargo_type_name` string comment '货物类型名称',
	`order_count` bigint comment '下单数',
	`order_amount` decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单 1 日汇总表'
	partitioned by(`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trade_org_cargo_type_order_1d'
	tblproperties('orc.compress' = 'snappy');

