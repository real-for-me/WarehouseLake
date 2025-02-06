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

create  DATABASE IF NOT EXISTS hudi_dws;

CREATE TABLE IF NOT EXISTS hudi_dws.dws_trade_user_sku_order_refund_nd_full(
    `user_id`                     BIGINT COMMENT '用户id',
    `sku_id`                      BIGINT COMMENT 'sku_id',
    `k1`                          STRING COMMENT '分区字段',
    `sku_name`                    STRING COMMENT 'sku名称',
    `category1_id`                BIGINT COMMENT '一级分类id',
    `category1_name`              STRING COMMENT '一级分类名称',
    `category2_id`                BIGINT COMMENT '一级分类id',
    `category2_name`              STRING COMMENT '一级分类名称',
    `category3_id`                BIGINT COMMENT '一级分类id',
    `category3_name`              STRING COMMENT '一级分类名称',
    `tm_id`                       BIGINT COMMENT '品牌id',
    `tm_name`                     STRING COMMENT '品牌名称',
    `order_refund_count_30d`      BIGINT COMMENT '最近30日退单次数',
    `order_refund_num_30d`        BIGINT COMMENT '最近30日退单件数',
    `order_refund_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日退单金额',
    PRIMARY KEY (`user_id`,`sku_id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

INSERT INTO hudi_dws.dws_trade_user_sku_order_refund_nd_full(
    user_id,
    sku_id,
    k1,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_refund_count_30d,
    order_refund_num_30d,
    order_refund_amount_30d
    )
select
    user_id,
    sku_id,
    k1,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(order_refund_count_1d),
    sum(order_refund_num_1d),
    sum(order_refund_amount_1d)
from hudi_dws.dws_trade_user_sku_order_refund_1d_full
group by user_id,sku_id,k1,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;