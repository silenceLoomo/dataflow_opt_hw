# dataflow_opt_hw
大作业3

说明：
首先执行RunTest.scala程序，从s3获取数据并传入kafka的对应主题中

然后执行ComsumerTest.scala程序，从kafka中获取对应主题，并进行消费，最后输出到s3中

s3Writer.scala是朝s3写入文件的实现类

流程：

1.执行RunTest.scala程序
功能：从s3获取数据、将数据传入kafka

2.执行ComsumerTest.scala程序
功能：创建kafka消费者、消费数据、将数据存入s3
