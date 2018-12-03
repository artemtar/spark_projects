
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
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

//rest
import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector}
import scala.collection.mutable.WrappedArray

object MyConfig {
  val NUM_OF_AUTHORS: Integer = 10
  val TRAINING_PORTION = 0.9
  val VALIDATING_PORTION = 0.1
  var MINIUM_NUMBER_OF_POSTS: Integer = 0
  val VECTOR_SIZE = 300
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
    
  private var validationSet: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = _
  private var model: org.apache.spark.mllib.classification.LogisticRegressionModel = _
    
  val LogReg = new LogisticRegressionWithLBFGS()
                                      .setNumClasses(MyConfig.NumClasses)

  def makeTestSet() = {
    val rdd = df.rdd
    val regData = rdd.map(row => { val lable = if (row.getAs[String]("reviewerID") == AuthourId) 1 else 0
                        val vector = row.getAs[org.apache.spark.ml.linalg.SparseVector]("result").toDense
                        LabeledPoint(lable, org.apache.spark.mllib.linalg.Vectors.fromML(vector))})
    regData.randomSplit(Array(training_size.toDouble, validating_size.toDouble), 11L)
  }
    
  def makeTestSetSq() = {
    var count = 50
    val rdd = df.rdd
    val regData = rdd.map(row => { val lable = if (row.getAs[String]("reviewerID") == AuthourId) 1 else 0
                        val vector = row.getAs[org.apache.spark.ml.linalg.SparseVector]("result").toDense                        
                        LabeledPoint(lable, org.apache.spark.mllib.linalg.Vectors.fromML(vector))})
      
    val testSize = (count * validating_size).toInt
    val trainSize = (count * training_size).toInt * 10
    var countTrain = 0
    val trainTestData = regData.map{case LabeledPoint(lable, vector) => 
        if (lable == 1)
//             || (countTrain < count * 10))
//         {countTrain = countTrain + 1 
        LabeledPoint(lable, vector)}
      println(count)
      println(countTrain)
      trainTestData
//     trainTestData.randomSplit(Array(training_size.toDouble, validating_size.toDouble), 11L)      
  }
  
  def train(data: Array[org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]]) = {
    
    LogReg.optimizer.setRegParam(0.0)
    val train = data(0).cache()
    validationSet = data(1)
    val model_author = LogReg.run(train)
      model = model_author
    model_author
    
  }
    def getMetrics() = {
      val predictionAndLabels = validationSet.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)}
          
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC = " + auROC)
    println("Sensitivity = " + predictionAndLabels.filter(x => x._1 == x._2 && x._1 == 1.0).count().toDouble / predictionAndLabels.filter(x => x._2 == 1.0).count().toDouble)
    println("Specificity = " + predictionAndLabels.filter(x => x._1 == x._2 && x._1 == 0.0).count().toDouble / predictionAndLabels.filter(x => x._2 == 0.0).count().toDouble)
    println("Accuracy = " + predictionAndLabels.filter(x => x._1 == x._2).count().toDouble / predictionAndLabels.count().toDouble)
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
      msgCleaned.rdd.map(row => { 
                            val vector = row.getAs[org.apache.spark.ml.linalg.SparseVector]("result").toDense
                            org.apache.spark.mllib.linalg.Vectors.fromML(vector)})  
    }
  
    def cleanData(dfDirty: DataFrame): DataFrame = {
//     val s1 = dfDirty.filter(col("raw").like("_"))
                            
//       x != ':' || x != ';' || x != '/'  || x != '-' || x != '_' || x != ')' || x != '(' || x != '>' || x != '<')
    
   // val s2 = dfDirty.withColumn("rawTrimed", cleaningUDF(col("raw")))
    val noStopWords = remover.transform(dfDirty)
    val model = toVec.fit(noStopWords)
    model.transform(noStopWords)
    }


    def userInput(session: SparkSession) = {
      
        var msg = readLine("Type the msg: ")
        while (msg != "stop"){
            msgToVec(msg, session)
            readLine("Type the msg: ")
        }    
    }
    
}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

// object SparkSessionCreate { 
//   val createSession: SparkSession = { 
//       SparkSession
//       .builder()
//       .appName("project")
//       .master("local")
//       .getOrCreate()
//   }
// }

object SQLContextSingleton {

  @transient private var instance: SparkSession = _

  def createSession(): SparkSession = {
    if (instance == null) {
      instance = SparkSession
      .builder()
      .appName("project")
      .master("local")
      .getOrCreate()
    }
    instance
  }
}

def writeTofile(df: DataFrame, path: String){
  df.coalesce(1)
  .write
  .mode(SaveMode.Overwrite)
  .json(path)
}


import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.classification.LogisticRegressionModel
import scala.io.Source._


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
  def save() = {
    val path: String = "/home/artemtarasov89/models"
    val writer = new PrintWriter(path + "/models.txt")
         authorModelList.map(e => 
                          {
                            val model = e._2
                            val modelName = e._1
                            writer.append(modelName.asInstanceOf[String] + ".")
                            model.save(Controller.getSession.sparkContext, path + "/" + modelName)
                          })
    writer.close
}

}

object Controller{  
  
  def topReviewrsToList(topRev: DataFrame) = {
    val selectList = "reviewerID"
    val dfTopReviers = topRev.select(selectList)
    (dfTopReviers.collect).toSet
  }  
  
  def cleanEachTopAuthor(dfTopReviers: DataFrame) = {
    val cleaner = DataPrep(dfTopReviers.select("reviewerID", "reviewText"))
    val tokenized = cleaner.tokenize("reviewText")
    cleaner.cleanData(tokenized)    
  }
  
  def makeAuthorModel(authourId: Any, cleanData: DataFrame) = {
    val author = AuthorModel(authourId.asInstanceOf[String], cleanData, 0.8, 0.2)
    val authorTestTrainSet = author.makeTestSet()      
    author.train(authorTestTrainSet)
  }
  
  def getTrainedModelList(trainSet: DataFrame, dfTopReviers: DataFrame) = {  
    val authorList = (topReviewrsToList(dfTopReviers)).toArray
    val authorModels = authorList
                  .map(e => (e(0), makeAuthorModel(e(0), trainSet)))
    authorModels
  }
  def getSession() = {SQLContextSingleton.createSession}
  
  def main() = {
      val session: SparkSession = getSession()
      val path: String = "/user/artemtarasov89/333.json"
      val data = Data(path)
      val selectList = Seq("reviewerID", "reviewerName", "reviewText")
    
      val df = data.jsonToDF(session)
      val topReviewrMaker = new Reviewers(df)
      val dfTopReviers = topReviewrMaker.top(selectList, 1)
      val mainDataSet = cleanEachTopAuthor(df)
      val arrayModel = getTrainedModelList(mainDataSet.select("reviewerID", "result"), dfTopReviers)
      Classifier(arrayModel)
  }
  
  def load(): Classifier = {
    val path: String = "/home/artemtarasov89/models"
    val models = fromFile(path + "/models.txt").mkString
    val modelsList = models.split('.')
    val authorModelsList = modelsList.map(e => {
                                          val model = org.apache.spark.mllib.classification.LogisticRegressionModel
                                          .load(Controller.getSession.sparkContext, path + "/" + e)
                                            (e.asInstanceOf[Any], model)})
  Classifier(authorModelsList)
}
}

val session: SparkSession = SQLContextSingleton.createSession()
val path: String = "/user/artemtarasov89/333.json"
val data = Data(path)
val selectList = Seq("reviewerID", "reviewerName", "reviewText")    
val df = data.jsonToDF(session)

df.select("asin", "reviewText", "reviewerID", "reviewerName", "summary").show()

val mainDataSet = Controller.cleanEachTopAuthor(df)

mainDataSet.select("result").show()

val authorArrayModels = Controller.load()

authorArrayModels.authorModelList

val session = Controller.getSession
val msgMkr = MsgMaker()
val msg = msgMkr.msgToVec("Hello I am spark.", session)

authorArrayModels.classify(msg)

val dff = newdf.rdd
dff.take(1)

def cleanSpecific(df: DataFrame) = {
    df.rdd
//       .map(row => (row.getAs[Seq[String]]("reviewerID")))
//                    , row.getAs[String]("filtered")))
//       .filter(m => m._1.nonEmpty)
//       .filter(m => !m._2.equalsIgnoreCase("Anonimow"))
//       .map(r => Message(InputCleanUtil.clearWords(r.words).filter(_.length > 2),
//         r.author, toMillis(r), r.messaeId, r.subject)
//       )
  }

var i = 6
val j = 0.3
(i * j).toInt
i = i + 1
i
