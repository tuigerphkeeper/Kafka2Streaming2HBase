import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

object SendData {

  @throws[InterruptedException]
  def main(args: Array[String]) {
    val props = new Properties
    props.put("bootstrap.servers", "lyj1.bigdata:6667,lyj2.bigdata:6667,lyj3.bigdata:6667")
    props.put("acks", "1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val str =
      """[{time=1542246296680,yc_231=202.6999969482422;1,
        |yc_230=10.100000381469727;1, yc_211=-76.33999633789062;1, yc_233=77.11000061035156;1,
        |yc_210=138.89999389648438;1, yc_232=54.54999923706055;1, yc_213=-89.13999938964844;1,
        |yc_235=111.08000183105469;1, yc_212=-22.3799991607666;1, yc_234=148.5800018310547;1,
        |yc_204=8.920000076293945;1, yc_226=-30.559999465942383;1, yc_203=76.5;1,
        |yc_225=-46.27000045776367;1, yc_206=-37.119998931884766;1, yc_228=87.0199966430664;1,
        |yc_205=30.690000534057617;1, yc_227=139.55999755859375;1, yc_208=26.670000076293945;1,
        |yc_207=7.889999866485596;1}]""".stripMargin
    val time1 = System.currentTimeMillis
    for (i <- 0 to 1000000) {
      producer.send(new ProducerRecord("structured", String.valueOf(i), str))
      Thread.sleep(1)
    }
    val time2 = System.currentTimeMillis
    val time = time2 - time1
    System.out.println("信息发送完毕，共1000000条数据,耗时：" + time / 1000 + "秒")
    producer.close()
  }
}
