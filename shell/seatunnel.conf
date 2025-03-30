# seatunnel版本声明
# seatunnel version 2.3.10

# 环境配置
env {
    # 作业并行度，可根据集群资源和任务量调整
    parallelism = 2
    # 作业模式，这里设置为批量模式
    job.mode = "BATCH"
}

# 数据源配置
source {
    Jdbc {
        # MySQL数据库连接URL，包含数据库地址、端口、数据库名及相关参数
        url = "jdbc:mysql://10.39.48.36:3306/gmall_v_ChenMing?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true"
        # MySQL JDBC驱动类
        driver = "com.mysql.cj.jdbc.Driver"
        # 连接检查超时时间（秒）
        connection_check_timeout_sec = 100
        # 数据库用户名
        user = "root"
        # 数据库密码
        password = "Zh1028,./"
        # 要执行的SQL查询语句，这里以查询所有数据为例，可根据实际需求修改
        query = "select * from order_info"
    }
}

# 数据转换配置，这里暂时不进行转换，可根据需求添加
transform {

}

# 数据下沉配置，落地到 Hive
sink {
    Hive {
        table_name = "dev_realtime_test_chenming.ods_order_info"
        metastore_uri = "thrift://cdh02:9083"
        hive.hadoop.conf-path = "/etc/hadoop/conf"
        save_mode = "overwrite"
    }
}