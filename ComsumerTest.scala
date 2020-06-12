import java.util.{Properties, UUID}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010}
import com.bingocloud.{ClientConfiguration, Protocol}
import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.nlpcn.commons.lang.util.IOUtil


object ComsumerTest {
  //kafka参数
  //输入的kafka主题名称
  val inputTopic = "hzg_manual3_homework"
  //kafka地址
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"
  //s3参数
  val accessKey = "440E02A3AA89BA9D1355"
  val secretKey = "WzJBNjQzQjFGMEQzRjJGMjE0MjJBMUZGREU2Mzc5QUJFRUE3OEZFMEZd"
  //s3地址
  val endpoint = "huangzhenghong.scuts3.depts.bingosoft.net:29999"
  //上传到的桶
  val bucket = "huangzhenghong"
  //上传文件的路径前缀
  val keyPrefix = "manual3/"
  //上传数据间隔 单位毫秒
  val period = 5000



  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", bootstrapServers)
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new FlinkKafkaConsumer010[String](inputTopic,
      new SimpleStringSchema, kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputKafkaStream = env.addSource(kafkaConsumer)
    inputKafkaStream.writeUsingOutputFormat(new s3Writer(accessKey, secretKey, endpoint, bucket, keyPrefix, period))
    env.execute()
  }


}
