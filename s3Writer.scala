import java.io.{File, FileWriter}
import java.util.{Timer, TimerTask}
import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.{ClientConfiguration, Protocol}
import com.bingocloud.services.s3.AmazonS3Client
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import scala.collection.mutable.Set
import scala.util.parsing.json.JSON

class s3Writer(accessKey: String, secretKey: String, endpoint: String, bucket: String, keyPrefix: String, period: Int) extends OutputFormat[String] {
  var timer: Timer = _
  var file: File = _
  var fileWriter: FileWriter = _
  var length = 0L
  var amazonS3: AmazonS3Client = _
  val mutableSet:Set[String] = Set() //用于记录有新增内容的destination

  def parseJson(json:Option[Any]) :Map[String,Any] = json match {
    case Some(map: Map[String, Any]) => map
  }

  def getDest(x:String): String ={
    try{
      val jsonJ = JSON.parseFull(x)
      return parseJson(jsonJ).get("destination").toString
    } catch{
      case ex: Exception => {
        return null
      }
    }
  }

  def upload: Unit = {
    /*this.synchronized {
      if (length > 0) {
        fileWriter.close()
        val targetKey = keyPrefix + System.nanoTime()
        amazonS3.putObject(bucket, targetKey, file)
        println("开始上传文件：%s 至 %s 桶的 %s 目录下".format(file.getAbsoluteFile, bucket, targetKey))
        file = null
        fileWriter = null
        length = 0L
      }
    }*/
    this.synchronized {
      mutableSet.foreach(dest => {
        val targetKey = keyPrefix + dest + ".txt"
        val file = new File(dest + ".txt")
        amazonS3.putObject(bucket, targetKey, file)
        println("开始上传文件：%s 至 %s 桶的 %s 目录下".format(file.getAbsoluteFile, bucket, targetKey))
      })
      mutableSet.clear()
    }
  }

  override def configure(configuration: Configuration): Unit = {
    timer = new Timer("S3Writer")
    timer.schedule(new TimerTask() {
      def run(): Unit = {
        upload
      }
    }, 1000, period)
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)

  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {

  }

  override def writeRecord(it: String): Unit = {
    /*this.synchronized {
      if (StringUtils.isNoneBlank(it)) {
        if (fileWriter == null) {
          file = new File(System.nanoTime() + ".txt")
          fileWriter = new FileWriter(file, true)
        }
        fileWriter.append(it + "\n")
        length += it.length
        fileWriter.flush()
      }
    }*/
    val dest: String = getDest(it)
    if(dest == null)
      return
    else if(StringUtils.isNoneBlank(it) == false)
    return
    this.synchronized {
      val fileWriter = new FileWriter(new File(dest + ".txt"), true)
      fileWriter.append(it + "\n")
      fileWriter.flush()
      fileWriter.close()
      mutableSet.add(dest)
    }
  }


  override def close(): Unit = {
    timer.cancel()
  }
}