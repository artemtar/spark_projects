
object Cells {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._
  // io
  import scala.io._
  import com.fasterxml.jackson.databind.ObjectMapper
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
  // end io
  
  class Data(val path: String) {
    
    def load(session: SparkSession, header: Boolean, schema: Boolean, separator: String): DataFrame = 
      session.read.option("header", "true")
      .option("infereschema", "true")
      .option("sep", separator)
      .csv(path) 
    
    def loadjson(session: SparkSession) = {
      val json = Source.fromFile(path)
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      val parsedJson = mapper.readValue[Map[String, Object]](json.reader())
      parsedJson
    }
    
    def jsonToDF(session: SparkSession) = {
      session.read.json(path)
    }
  }
  object SparkSessionCreate { 
    val createSession: SparkSession = { 
        SparkSession
        .builder()
        .appName("project")
        .master("local")
        .getOrCreate()
    }
  }

  /* ... new cell ... */

  val session: SparkSession = SparkSessionCreate.createSession
  val path: String = "/home/atarasov/Documents/111/test.json"
  val data = new Data(path)

  /* ... new cell ... */

  import session.implicits._
  val df = data.jsonToDF(session)
  val df_filtered = df.select("reviewerID", "reviewerName", "reviewText", "summary")
  val reviewers = df_filtered.groupBy("reviewerID").count()
  val top_reviewers = reviewers.sort(desc("count")).take(20)

  /* ... new cell ... */

  val top_reviewers_list = top_reviewers.map(x => x(0))

  /* ... new cell ... */

  val contains: String => Boolean =
}
                  