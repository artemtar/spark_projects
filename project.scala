
object Cells {
  //spark staff
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._
  
  
  // io
  import scala.io._
  import com.fasterxml.jackson.databind.ObjectMapper
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
  import org.apache.spark.sql.SaveMode
  
  // ml
  import org.apache.spark.ml.feature.{HashingTF, Tokenizer, StopWordsRemover, Word2Vec}
  import scala.collection.mutable.WrappedArray
  import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
  import org.apache.spark.mllib.regression.LabeledPoint
  
  //rest
  import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector}
  
  object MyConfig {
    val NUM_OF_AUTHORS: Integer = 10
    val TRAINING_PORTION = 0.9
    val VALIDATING_PORTION = 0.1
    var MINIUM_NUMBER_OF_POSTS: Integer = 0
    val NumClasses = 2
  }
  
  case class Data(val path: String) {
    
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
  
  case class Reviewers(val unsorted: DataFrame){
    
    def top(selctOption: Seq[String], limit: Integer): DataFrame = {
      val args = listToCol(selctOption)
      val filtered = unsorted.select(args:_*)
      val topCount = makeTop(filtered, limit, "reviewerID", "reviewerID")
      filtered.join(topCount, col("reviewerID") === col("id"))
    }
    
    def listToCol(selctOption: Seq[String]): Seq[org.apache.spark.sql.Column] = {
      selctOption.map(name => col(name))
    }
    
    def makeTop(filtered: DataFrame, lim: Integer, group: String, Id: String) = {
    filtered.groupBy(group)
          .count()
          .sort(desc("count"))
          .limit(lim)
          .select(col(Id).alias("id"))
    }
  }
  
  case class DataPrep(val df: DataFrame){
    
    def MakeTokenizer(input: String) = {
      new Tokenizer()
    .setInputCol(input)
    .setOutputCol("raw")
    }
    def MakeStopWordRemover(input: String) = {
     new StopWordsRemover()
    .setInputCol(input)
    .setOutputCol("filtered")
    }
    def getW2V(input: String) = {
        new Word2Vec()
        .setInputCol(input)
        .setOutputCol("result")
        .setVectorSize(300)
        .setMinCount(0)
    }  
    
    def tokenize(input: String): DataFrame = {
      val tokenizer = MakeTokenizer(input)
      tokenizer.transform(df)
    } 
    
    val cleaning: WrappedArray[String] => Array[String] = (v: WrappedArray[String]) => {
      
      val unwraped = v.asInstanceOf[WrappedArray[String]].toArray
      val removeSpaseLowerCase =
        for(e <- unwraped if !e.isEmpty())
          yield e.trim.toLowerCase()
      .replaceAll( """<(?!\/?a(?=>|\s.*>))\/?.*?>""", "")
      removeSpaseLowerCase
  //     .replaceAll("\\<[^>]*>", "")
  }
    val cleaningUDF = 
      udf[Array[String], WrappedArray[String]](cleaning)
    
    def cleanData(dfDirty: DataFrame): DataFrame = {
  //     val s1 = dfDirty.filter(col("raw").like("_"))
                              
  //       x != ':' || x != ';' || x != '/'  || x != '-' || x != '_' || x != ')' || x != '(' || x != '>' || x != '<')
      
      val s2 = dfDirty.withColumn("rawTrimed", cleaningUDF(col("raw")))
      val remover = MakeStopWordRemover("rawTrimed")
      val noStopWords = remover.transform(s2)
      val vector = getW2V("filtered")
      val model = vector.fit(noStopWords)
      model.transform(noStopWords)
    }
  
    }  
    
  
  
  case class AuthorModel(val AuthourId: String,
                         val df: DataFrame,
                         val training_size: Double,
                         val validating_size: Double){
    
    val LogReg = new LogisticRegressionWithLBFGS()
                                        .setNumClasses(MyConfig.NumClasses)
  
    def makeTestSet() = {
      val rdd = df.rdd
      val regData = rdd.map(row => { val lable = if (row.getAs[String]("reviewerID") == AuthourId) 1 else 0
                          val vector = row.getAs[org.apache.spark.ml.linalg.SparseVector]("result").toDense
                          LabeledPoint(lable, org.apache.spark.mllib.linalg.Vectors.fromML(vector))})
      regData.randomSplit(Array(training_size.toDouble, validating_size.toDouble), 11L)
    }
    
    def train(data: Array[org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]]) = {
      
      LogReg.optimizer.setRegParam(0.0)
      val train = data(0).cache()
      val validate = data(1).cache()
      val model = LogReg.run(train)
      model
      
    }
    
  }
  
  def msgToVector(msg: String) = {
    
    val tokenizer = new Tokenizer()
    val remover = new StopWordsRemover()  
    val getW2V = new Word2Vec().setVectorSize(300)
    
  
  //     tokenizer.transform(msg)
  
  //   val cleaning: WrappedArray[String] => Array[String] = (v: WrappedArray[String]) => {
      
  //     val unwraped = v.asInstanceOf[WrappedArray[String]].toArray
  //     val removeSpaseLowerCase =
  //       for(e <- unwraped if !e.isEmpty())
  //         yield e.trim.toLowerCase()
  //     .replaceAll( """<(?!\/?a(?=>|\s.*>))\/?.*?>""", "")
  //     removeSpaseLowerCase
  // }
    
  }
  
  case class MsgMaker() {    
  
    val tokenizer = 
      new Tokenizer()
    .setInputCol("msg")
    .setOutputCol("raw")
    
    val remover = 
     new StopWordsRemover()
    .setInputCol("raw")
    .setOutputCol("filtered")
    
    val toVec = 
        new Word2Vec()
        .setInputCol("filtered")
        .setOutputCol("result")
        .setVectorSize(300)
        .setMinCount(0)  
    
    
      val cleaning: WrappedArray[String] => Array[String] = (v: WrappedArray[String]) => {
      
      val unwraped = v.asInstanceOf[WrappedArray[String]].toArray
      val removeSpaseLowerCase =
        for(e <- unwraped if !e.isEmpty())
          yield e.trim.toLowerCase()
      .replaceAll( """<(?!\/?a(?=>|\s.*>))\/?.*?>""", "")
      removeSpaseLowerCase
  //     .replaceAll("\\<[^>]*>", "")
  }
    val cleaningUDF = 
      udf[Array[String], WrappedArray[String]](cleaning)
    
  
      def msgToVec(msg: String, session: SparkSession ) = {    
        import session.implicits._
        val msgDf = Seq(msg).toDF("msg")
        val tokenized = tokenizer.transform(msgDf)
        val msgCleaned = cleanData(tokenized)
        msgCleaned.rdd.map(row => { val lable = 0
                              val vector = row.getAs[org.apache.spark.ml.linalg.SparseVector]("result").toDense
                              org.apache.spark.mllib.linalg.Vectors.fromML(vector)})  
      }
    
      def cleanData(dfDirty: DataFrame): DataFrame = {
  //     val s1 = dfDirty.filter(col("raw").like("_"))
                              
  //       x != ':' || x != ';' || x != '/'  || x != '-' || x != '_' || x != ')' || x != '(' || x != '>' || x != '<')
      
      val s2 = dfDirty.withColumn("rawTrimed", cleaningUDF(col("raw")))
      val noStopWords = remover.transform(s2)
      val model = toVec.fit(noStopWords)
      model.transform(noStopWords)
    }
  
  
  def userInput(session: SparkSession) = {
    val msg = readLine("Type the msg: ")
    msgToVec(msg, session)
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
  
  def writeTofile(df: DataFrame, path: String){
    df.coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .json(path)
  }

  /* ... new cell ... */

  val session: SparkSession = SparkSessionCreate.createSession
  val path: String = "/home/atarasov/Documents/111/test.json"
  val data = Data(path)    

  /* ... new cell ... */

  val selectList = Seq("reviewerID", "reviewerName", "reviewText", "summary")

  /* ... new cell ... */

  val df = data.jsonToDF(session)
  val topReviewrMaker = new Reviewers(df)
  val dfTopReviers = topReviewrMaker.top(selectList, 10)

  /* ... new cell ... */

  val cleaner = DataPrep(dfTopReviers)
  val tok = cleaner.tokenize("reviewText")
  val a = cleaner.cleanData(tok)
  val b = a.select("raw", "rawTrimed", "filtered", "result")
  val m = AuthorModel("AV6QDP8Q0ONK4", a, 0.9, 0.1)
  val mm = m.makeTestSet()
  val model = m.train(mm)
  
  // writeTofile(b, "/home/atarasov/Documents/111/out.json")

  /* ... new cell ... */

  import session.implicits._
  val mm = MsgMaker()
  val i = mm.msgToVec("should this match", session)

  /* ... new cell ... */

  model.predict(i).collect

  /* ... new cell ... */

  
  val f = i.map({case LabeledPoint(label, features) => features})

  /* ... new cell ... */

  model
  model.clearThreshold()
  val res = model.predict(f)
  res.collect

  /* ... new cell ... */

  val nr = session.read.json(path)
  val nf = nr.select("reviewerID", "reviewerName", "reviewText", "summary")
  val reviewers = nr.groupBy("reviewerID").count()
  reviewers.count

  /* ... new cell ... */

  val top_reviewers = reviewers.sort(desc("count")).limit(10)
  val ress = top_reviewers.select($"count", $"reviewerID".alias("id"))
  val post_num = ress.select("count").collect()(NUM_OF_AUTHORS - 1)
  MINIUM_NUMBER_OF_POSTS = post_num(0).asInstanceOf[Long].toInt
  
  ress.show

  /* ... new cell ... */

  Vector(1)

  /* ... new cell ... */

  val r = ress.rdd
  val s = "AV6QDP8Q0ONK4"
  val rr = r.map(row => { val lable = if (row.getAs[String]("id") == s) 1 else 0
                          val vector = Vectors(row.getAs[Float]("count"))
                          LabeledPoint(lable, vector)})
  rr.take(10)

  /* ... new cell ... */

  val another = nr.join(ress, $"reviewerID" === $"id")
  val tt = new Tokenizer()
    .setInputCol("reviewText")
    .setOutputCol("raw")

  /* ... new cell ... */

  import scala.collection.mutable.WrappedArray
  val yetanother = tt.transform(another).select("reviewText", "raw")
  val removeWhiteSpaceAndLowerCase: WrappedArray[String] => Array[String] = (v: WrappedArray[String]) => {
    val n: Array[String] = Array[String]()
    val unwraped = v.asInstanceOf[WrappedArray[String]].toArray
    for(e <- unwraped){n + e.toLowerCase().trim}
    n}
    
    val removeWhiteSpaceAndLowerCaseUDF = udf[Array[String], WrappedArray[String]](removeWhiteSpaceAndLowerCase)
  // yetanother.rdd.first
  yetanother.withColumn("rawTrimed", removeWhiteSpaceAndLowerCaseUDF(yetanother("raw")))
  // yetanother.collect()(1)

  /* ... new cell ... */

  

  /* ... new cell ... */

  session.stop
}
                  
