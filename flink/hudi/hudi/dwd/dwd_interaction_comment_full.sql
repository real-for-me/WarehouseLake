SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_dwd;

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_interaction_comment_full(
    `id`            BIGINT COMMENT '编号',
    `k1`            STRING COMMENT '分区字段',
    `user_id`       BIGINT COMMENT '用户ID',
    `sku_id`        BIGINT COMMENT 'sku_id',
    `order_id`      BIGINT COMMENT '订单ID',
    `date_id`       STRING COMMENT '日期ID',
    `create_time`   timestamp(3) COMMENT '评价时间',
    `appraise_code` STRING COMMENT '评价编码',
    `appraise_name` STRING COMMENT '评价名称',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );


insert into hudi_dwd.dwd_interaction_comment_full(
    id,
    k1,
    user_id,
    sku_id,
    order_id,
    date_id,
    create_time,
    appraise_code,
    appraise_name
)
select
    id,
    k1,
    user_id,
    sku_id,
    order_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    appraise,
    dic_name
from
    (
        select
            id,
            k1,
            user_id,
            sku_id,
            order_id,
            create_time,
            appraise
        from hudi_ods.ods_comment_info_full
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from hudi_ods.ods_base_dic_full
    )dic
    on ci.appraise=dic.dic_code;