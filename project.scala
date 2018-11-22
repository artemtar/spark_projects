
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
  import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
  import org.apache.spark.mllib.regression.LabeledPoint
  
  //rest
  import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector}
  import scala.collection.mutable.WrappedArray
  
  object MyConfig {
    val NUM_OF_AUTHORS: Integer = 10
    val TRAINING_PORTION = 0.9
    val VALIDATING_PORTION = 0.1
    var MINIUM_NUMBER_OF_POSTS: Integer = 0
    val VECTOR_SIZE = 100
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
        .setVectorSize(MyConfig.VECTOR_SIZE)
        .setMinCount(0)
    }  
    
    def tokenize(input: String): DataFrame = {
      val tokenizer = MakeTokenizer(input)
      tokenizer.transform(df)
    } 
    
    val cleaning: WrappedArray[String] => Array[String] = (v: WrappedArray[String]) => {
      v.toArray
  //     val unwraped = v.asInstanceOf[WrappedArray[String]].toArray
  //     val removeSpaseLowerCase: Array[String] =
  //       for(e <- unwraped if !e.isEmpty())
  //         yield e.trim.toLowerCase()
  //     .replaceAll( """<\(?!\/?a(?=>|\s.*>\)\\)\/?.*?>""", "")
  //   removeSpaseLowerCase
  //   val result = Array[String]()
    
  //   for(word <- removeSpaseLowerCase){
  //     if (word.contains(".") && (word.count(_ == '.') == 1) && !(word.takeRight(1) == "."))
  //         {
  //           println(word)
  //           var splited = word.split('.')
  //           result = result :+ (splited(0) + ".")
  //           result = result :+ splited(1)
  //         }  
  //     else result = result :+ word
  //   }
  //   result
  }
    
    val cleaningUDF = 
      udf[Array[String], WrappedArray[String]](cleaning)
    
    def cleanData(dfDirty: DataFrame): DataFrame = {
  //     val s1 = dfDirty.filter(col("raw").like("_"))
                              
  //       x != ':' || x != ';' || x != '/'  || x != '-' || x != '_' || x != ')' || x != '(' || x != '>' || x != '<')
      
  //     val s2 = dfDirty.withColumn("rawTrimed", cleaningUDF(col("raw")))
      val remover = MakeStopWordRemover("raw")
      val noStopWords = remover.transform(dfDirty)
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
        .setVectorSize(MyConfig.VECTOR_SIZE)
        .setMinCount(0)  
    
    
    val cleaning: WrappedArray[String] => Array[String] = (v: WrappedArray[String]) => {
      
      val unwraped = v.asInstanceOf[WrappedArray[String]].toArray
      val removeSpaseLowerCase: Array[String] =
        for(e <- unwraped if !e.isEmpty())
          yield e.trim.toLowerCase()
      .replaceAll( """<\(?!\/?a(?=>|\s.*>\)\\)\/?.*?>""", "")
    
  //   var result = Array[String]()
    
  //   for(word <- removeSpaseLowerCase){
  //     if (word.contains(".") && (word.count(_ == '.') == 1) && !(word.takeRight(1) == "."))
  //         {
  //           println(word)
  //           var splited = word.split('.')
  //           result = result :+ (splited(0) + ".")
  //           result = result :+ splited(1)
  //         }  
  //     else result = result :+ word
  //   }
  //   result
      removeSpaseLowerCase
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
  import org.apache.spark.sql.{DataFrame, SQLContext}
  import org.apache.spark.{SparkConf, SparkContext}
  
  object SparkSessionCreate { 
  //     val conf = new SparkConf().setAppName("project")
  //     val sc: SparkContext = new SparkContext(conf)
  //     val createSession: SQLContext = new SQLContext(sc)
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

  import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._
  import org.apache.spark.mllib.classification.LogisticRegressionModel
  // import org.apache.spark.ml.classification.LogisticRegression
  
  
  case class Classifier(authorModelList: Array[(Any, LogisticRegressionModel)]){
    def classify(msg: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]) = {
      
      val predictions = authorModelList.map(e => 
                          {val model = e._2
                          model.clearThreshold()
                          val prediction = model.predict(msg)
                           (e._1, prediction.collect)})
      predictions.sortWith(_._2(0) > _._2(0))
    }
    
    def getPrediction(msg: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]) = {
      val arrayPrediction = classify(msg)
      arrayPrediction(0)._1
    }
    def save() = {}
  
  }
  
  object Controller{  
    
    def topReviewrsToList(topRev: DataFrame) = {
      val selectList = "reviewerID"
      val dfTopReviers = topRev.select(selectList)
      (dfTopReviers.collect).toSet
    }  
    
    def cleanEachTopAuthor(dfTopReviers: DataFrame) = {
      val cleaner = DataPrep(dfTopReviers)
      val tokenized = cleaner.tokenize("reviewText")
      cleaner.cleanData(tokenized)    
    }
    
    def makeAuthorModel(authourId: Any, cleanData: DataFrame) = {
      val author = AuthorModel(authourId.asInstanceOf[String], cleanData, 0.6, 0.4)
      val authorTestTrainSet = author.makeTestSet()
      author.train(authorTestTrainSet)
    }
    
    def getTrainedModelList(trainSet: DataFrame, dfTopReviers: DataFrame) = {  
      val authorList = (topReviewrsToList(dfTopReviers)).toArray
      val authorModels = authorList
                    .map(e => (e(0), makeAuthorModel(e(0), trainSet)))                       
  //     Classifier(authorModels)
      authorModels
    }
    def getSession() = {SparkSessionCreate.createSession}
    
    def main() = {
        val session: SparkSession = getSession()
        val path: String = "/home/atarasov/Documents/111/test.json"
        val data = Data(path)
        val selectList = Seq("reviewerID", "reviewerName", "reviewText", "summary")
      
        val df = data.jsonToDF(session)
        val topReviewrMaker = new Reviewers(df)
        val dfTopReviers = topReviewrMaker.top(selectList, 2)
      
        val mainDataSet = cleanEachTopAuthor(df)
        val arrayModel = getTrainedModelList(mainDataSet, dfTopReviers)
        Classifier(arrayModel)
    }
    def load() = {}
  }

  /* ... new cell ... */

  import scala.collection.mutable.WrappedArray
  val inp = Array[String]("new","years","eve,","four","people","bump","top","popular","suicide","building","intent","jumping.martin","sharp","(pierce","brosnan)","popular",
                          "morning","show","host","family.","fell","grace","&#34;she","looked","like","25.&#34;maureen",
                          "(toni","collette)","rather","mousey","person","never","lived","life.","cares","invalid","son","deeply","loves.jess","(imogen","poots)","daughter",
                          "rich","politician.","sister","jennifer,","read","book","invisibility,","ironically","went","missing","two","years","ago.","currently","mad",
                          "boyfriend","dumped","her.jj","(aaron","paul)","american","part","amateur","band.","claims","ccr","brain","cancer.the","press","found",
                          "suicide","attempts","gain","15","minutes","fame.","turn","form","bond","become","support","group.this","another","magnolia","&#34;feel",
                          "good&#34;","film.","seen","one","before,","know","feeling.","typically,","film","light","quirky","comedy,","messages","life","take","you,",
                          "decent","acting","directing.","rosamund","pike","role","smaller","billing.parental","guide:","f-bomb.","sex.","brief","rear","nudity","(poots)")
  val cleaning: WrappedArray[String] => Array[String] = (v: WrappedArray[String]) => {
      
      val unwraped = v.asInstanceOf[WrappedArray[String]].toArray
      val removeSpaseLowerCase: Array[String] =
        for(e <- unwraped if !e.isEmpty())
          yield e.trim.toLowerCase()
      .replaceAll( """<\(?!\/?a(?=>|\s.*>\)\\)\/?.*?>""", "")
    
    var result = Array[String]()
    
    for(word <- removeSpaseLowerCase){
      if (word.contains(".") && (word.count(_ == '.') == 1) && !(word.takeRight(1) == "."))
          {
            println(word)
            var splited = word.split('.')
            result = result :+ (splited(0) + ".")
            result = result :+ splited(1)
          }  
      else result = result :+ word
    }
    result
  }

  /* ... new cell ... */

  val session = Controller.getSession()
  val path: String = "/home/atarasov/Documents/111/test.json"
  val selectList = Seq("reviewerID", "reviewerName", "reviewText", "summary")
  val data = Data(path)    
  val df = data.jsonToDF(session)
  val topReviewrMaker = new Reviewers(df)
  val dfTopReviers = topReviewrMaker.top(selectList, 1)

  /* ... new cell ... */

  val cleaner = DataPrep(df)
  val tok = cleaner.tokenize("reviewText")
  val cl = cleaner.cleanData(tok)
  // val whatIwhantToselect = a.select("raw", "filtered", "result")
  cl.count
  
  // writeTofile(whatIwhantToselect.select("filtered", "raw"), "/home/atarasov/Documents/111/out.json")

  /* ... new cell ... */

  val m = AuthorModel("AV6QDP8Q0ONK4", a, 0.9, 0.1)
  val mm = m.makeTestSet()
  val model = m.train(mm)

  /* ... new cell ... */

  val session = Controller.getSession
  import session.implicits._
  val mm = MsgMaker()
  val i = mm.msgToVec("This show always is excellent, as far as british crime or mystery" +
  "showsgoes this is one of the best ever made." +
  "The stories are well done and the acting is top notch with interesting twists in the" +
  "realistic and brutal storylines." +
  "One episode is not on this disc the excellent 'prayer of the bone\" which is on a seperate disc.", session)

  /* ... new cell ... */

  val clas = Controller.main

  /* ... new cell ... */

  clas.classify(i)

  /* ... new cell ... */

  import java.io._
  
  def save(models: Array[(Any, org.apache.spark.mllib.classification.LogisticRegressionModel)]) = {
    val path: String = "/home/atarasov/Documents/111/models"
    val writer = new PrintWriter(path + "/models.txt")
         models.map(e => 
                          {
                            val model = e._2
                            val modelName = e._1
                            writer.append(modelName.asInstanceOf[String] + ".")
                            model.save(Controller.getSession.sparkContext, path + "/" + modelName)
                          })
    writer.close
  }

  /* ... new cell ... */

  import scala.io.Source._
  
  def load(): Classifier = {
    val path: String = "/home/atarasov/Documents/111/models"
    val models = fromFile(path + "/models.txt").mkString
    val modelsList = models.split('.')
    val authorModelsList = modelsList.map(e => {
                                          val model = org.apache.spark.mllib.classification.LogisticRegressionModel
                                          .load(Controller.getSession.sparkContext, path + "/" + e)
                                            (e.asInstanceOf[Any], model)})
    Classifier(authorModelsList)
  }

  /* ... new cell ... */

  val m = load()

  /* ... new cell ... */

  m.classify(i)

  /* ... new cell ... */

  save(clas.authorModelList)

  /* ... new cell ... */

  i.collect

  /* ... new cell ... */

  clas.classify(i)

  /* ... new cell ... */

  // val c = v.authorList(0)._2
  model.clearThreshold()
  model.predict(i).collect

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

  val top_reviewers = reviewers.sort(desc("count")).limit(20)
  val ress = top_reviewers.select($"count", $"reviewerID".alias("id"))
  val post_num = ress.select("count").collect()(MyConfig.NUM_OF_AUTHORS - 1)
  MyConfig.MINIUM_NUMBER_OF_POSTS = post_num(0).asInstanceOf[Long].toInt
  
  ress.show

  /* ... new cell ... */

  val what = ress.select("id").collect()
  val e = what.map(e => (e(0), 1))

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
                  