模块介绍
    DoWorkModule（知识点：SparkSQL风格分析业务）

    DSLModule（知识点：DSL风格对DS、DF进行操作）

    RealTimeModule（知识点：spartStreaming）
        ①主要是对实时业务进行分析
        ②从kafka消费数据并控制读取速度，背压机制、优雅关闭的配置等
        ③com.cwq.spark.sparkstream.writehdfs.RawLogSparkStreaming
            通用原始日志数据落盘到hdfs，每日产生一个文件
        ④手动保存偏移量（偏移量存在了MySQl中）
            CREATE TABLE `offset_manager`  (
              `groupid` varchar(50),
              `topic` varchar(50),
              `partition` int(11),
              `untiloffset` mediumtext,
              UNIQUE INDEX `offset_unique`(`groupid`, `topic`, `partition`)
            )
    UserModule（知识点：SparkSQL风格分析业务）
