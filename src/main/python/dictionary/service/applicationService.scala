import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileContext
import java.io.File
import java.io._
import scala.collection.mutable.ListBuffer
import java.time.{Instant,ZoneId,ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.util.matching
import util.control.Breaks._
import scala.concurrent.future
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.Map
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions
import org.apache.spark.sql.Row
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.sql.{Encoder,Encoders}
import spark.implicits._
import java.util.Calendar
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._
import java.util.Date
implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)
implicit def Tuple[A1,A2](implicit e1: Encoder[A1],e2 : Encoder[A2]

  ): Encoder[(A1, A2)] = Encoders.tuple[A1, A2](e1,e2)

//package com.alvinalexander.breakandcontinue
//import DataReader
//case class ODS_ITEM( ITEMID: Int, BUSINESSID: Int,SUBCATEGORYN: Int, ITEMNUMBER: Int, UNITPERPACKAGE : Int, STATUS : Int, ADDEDDATE : java.sql.Timestamp ,UPDATEDATE : java.sql.Timestamp, COUNTRYCODE : Int, OUTLETITEMMAPCHANGEDATE : java.sql.Timestamp)

//case class ODS_POSOUTLETITEMS(POIID : Int, BUSINESSID : Int, POSOUTLET : Int, OUTLETDIVISION : Int, OUTLETDEPARTMENT : Int, OUTLETCLASS : Int, OUTLETSUBCLASS : Int, OUTLETBRAND : Int, OUTLETBRANDNUMBER : Int, OUTLETDESCRIPTION : String, SKU : Int, ITEMID : Int, Price : Float)
@SerialVersionUID(100L)
object Main extends Serializable{

  def checking(x : Path, types : Int): Option[Path] = {
    val myContext : FileContext = FileContext.getFileContext()
    val v: FileStatus = myContext.getFileLinkStatus(x)
    val myFileSystem: FileSystem = FileSystem.get(sc.hadoopConfiguration)
    if(myFileSystem.exists(x) == false){
        return None
    }
    if(v.isDirectory && types == 0)
      return Some(x) //filesystemDiscovery(getPath(x.toString()))
  
    if(v.isFile && types == 1)
      return Some(x)
    return None
  }

  def filesystemDiscovery(listDir : ListBuffer[Path]): ListBuffer[Path] = {
      var r : ListBuffer[Path]= listDir.flatMap(x =>  checking(x,0) )
      var resultant: ListBuffer[Path] = listDir.flatMap(x => checking(x,1))
      if(r.length != 0){
        resultant ++= r.flatMap(x => filesystemDiscovery(getPath(x.toString())))
      }
      return resultant
  }


  def getDateTime(x : Path) : (Path, Date) = {
      val myContext : FileContext = FileContext.getFileContext()
      val filestatus: FileStatus = myContext.getFileLinkStatus(x)
      val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
      //format.format(new java.util.Date())
   
      return (x,format.parse(format.format(new java.util.Date(filestatus.getModificationTime()))))
  }

  def getPath(pathName : String): ListBuffer[Path] = {
    val dataFile = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(pathName)).map(x => x.getPath).toList
    return dataFile.to[ListBuffer]
  }

  def dateFormatting(date : String): (Int,Int,Int,Int,Int) = {
    val arr = date.split("-")
    val dayTime = arr(2).split("T")
    val time = dayTime(1).split(":")
    return (arr(0).toInt ,arr(1).toInt ,dayTime(0).toInt ,time(0).toInt ,time(1).toInt)
  }

  def sortingFunc( param1 : (Path,Int,Int,Int,Int,Int) , param2 : (Int,Int,Int,Int,Int)): Boolean = {
    //println(param1._3 + " " + param2._2)
    if(param1._2 != param2._1 ) return param1._2 > param2._1
    if(param1._3 != param2._2  ) return param1._3 > param2._2
    if(param1._4 != param2._3 ) return param1._4 > param2._3
    if(param1._5 != param2._4 ) return param1._5 > param2._4
    if( param1._6 != param2._5 ) return param1._6 > param2._5
    return false
  }
  def deleteOps(fileNameDate : ListBuffer[(Path,Int,Int,Int,Int,Int)]) : (ListBuffer[(Path,Int,Int,Int,Int,Int)],Int)= {
    println("Calling the deletion operation")
    val fs = FileSystem.get(sc.hadoopConfiguration) 
    fileNameDate.filter( x => x == None)
    fileNameDate.foreach(x => println(x._1))
    var setting = 0
    breakable {
      while(true){
        val input = readLine("Really want to delete the above files press y for deletion else n to avoid : ")
        println(input)
        if(input == "y" || input == "Y"){
            fileNameDate.map( x => fs.delete(x._1,false))
            setting = 1
            break
        }else if(input == "n" || input == "N"){
            println("FILES are not removed")
            break
        }
      }
    }
    println("End of removal process")
    
    return (fileNameDate,setting)
  }


  def searchOps(x : (Path,Int,Int,Int,Int,Int) , time : (Int, Int, Int, Int, Int),delPattern : scala.util.matching.Regex = "".r) : Option[(Path, Int, Int, Int, Int, Int)] = {
      var r1 = false
      if(sortingFunc(x,time) == true) r1 = true
      var dirElems = x._1.toString().split("/")
      
      var pattern = "/npd/rawdata.*".r
      if (delPattern != "")
        pattern = delPattern

      dirElems.filter( x => pattern.findAllIn(x).toList.length == 0 )
      if(r1 == true && dirElems.length != 0) return Some(x) else None

  }

  def directoryLatestFile(dir_ : String ): (Path,Path) = {
    val dataRdd = filesystemDiscovery(getPath(dir_))
    var fileNameDate = dataRdd.map(x => getDateTime(x))
    var latestTime : Date = new Date(0)
    var latestTime2 : Date = new Date(1)
    var dirName : Path  = new Path("dir_")
    var dirName2 : Path = new Path("dir_")
    /*
    for(eachFile <- fileNameDate){
      if(eachFile._2 > latestTime ){
        latestTime2 =  latestTime
        dirName2 = dirName
        latestTime = eachFile._2
        dirName = eachFile._1
      }else if (eachFile._2 > latestTime2){
        latestTime2 = eachFile._2
        dirName2 = eachFile._1
      }
    }
    */
    return (dirName , dirName2)
  } 

  /*
  def cleanupProcess(dir_ : String , time : String, sortOrSearch : Int): (ListBuffer[(Path,Int,Int,Int,Int,Int)],Int) = {
    val dataRdd = filesystemDiscovery(getPath(dir_))
  
    var fileNameDate = dataRdd.map(x => getDateTime(x)) 

    val dtFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
    val timeTups = dateFormatting(time)
    val fileNameDate_1 = fileNameDate.map( x => (x._1,dateFormatting(dtFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(x._2),ZoneId.of("UTC")))))).map(x => (x._1,x._2._1,x._2._2,x._2._3, x._2._4,x._2._5))
    var fileNameDate_2 : ListBuffer[(Path,Int,Int,Int,Int,Int)] =  fileNameDate_1.flatMap( x => searchOps(x,timeTups) )
    return deleteOps(fileNameDate_2)

  }*/
  def getFilteredColumn(df : DataFrame) : List[String] = {
    
        var dynamicList : ListBuffer[String] = new ListBuffer()
        dynamicList.append("ITEMID")
        dynamicList.append("BUSINESSID")
        dynamicList.append("ITEMNUMBER")
        dynamicList.append("UNITSPERPACKAGE")
        dynamicList.append("STATUS")
        dynamicList.append("ADDED")
        dynamicList.append("UPDATED")
        dynamicList.append("COUNTRY_CODE")
        return List("_c0 as ITEMID"," _c1 as BUSINESSID", "_c2 as ITEMNUMBER")
  } 
  // main entry point for this project this is a clean up process, directory discovery procesa
  /*
  @SerialVersionUID(100L)
  def main_(args : Array[String]) : Unit = {
    var i : Int = 0
    val dir_ = "/npd/ODS/ODS_INPUTS/ODS_POSITEMS"
    val subSetCols = Seq(0,1,2,3,4,104,105,106,112,116)
    val attribString  = "ITEMID, BUSINESSID,SUBCATEGORYN, ITEMNUMBER,UNITPERPACKAGE,STATUS, ADDEDDATE,UPDATEDATE"
    val tupR = directoryLatestFile(dir_)
    //val datareader1 = new DataReader(tupR._1)
    //val datareader2 = new DataReader(tupR._2)
    //val df1 = datareader1.reader
    //val df2 = datareader2.reader
    //var df3 = df1.unionAll(df2)
    val ctClass = new OdsContext()
    //val odsFunc : Row => ODS_ITEM = newAdd.odsItemFunc
    newAdd.mainOps(tupR, dir_, subSetCols, "ODS_ITEMS",attribString )
  }
*/
/*
  def main_2( dir_ : String, numOfColumns : Int, subsetCols : Seq[Int]): Unit = {
    var i : Int = 0
    //val dir_ = "/npd/ODS/ODS_INPUTS/ODS_POSITEMS"
    val dirComponents = dir_.split("/")
     //Future {
      var dict : Map[String, Int ] = Map()
      breakable{
      while(true){
        val tupR = directoryLatestFile(dir_)
        
          //if(dict.exists(_ == (tupR._1,1)) && dict.exists(_== (tupR._2,1)))
           //breac
          val datareader1 = new DataReader(tupR._1)
          val datareader2 = new DataReader(tupR._2)
          val df1 = datareader1.reader
          val df2 = datareader2.reader
          var df3 = df1.unionAll(df2)
          val dynamicList = getFilteredColumn(df3)
          val colNames = 0 to numOfColumns
          //val subsetCols = Seq(0,1,2,3,4,104,105,106,112,116)
          //df3.selectExpr(dynamicList)
          df3 =  df3.select(subsetCols map df3.columns map col: _*)
          df3.printSchema()
          println("Total records before filtering: " + df3.count())
          df3 = df3.rdd.map(attrib => ODS_ITEM(attrib.getInt(0),attrib.getInt(1),attrib.getInt(2),attrib.getInt(3),attrib.getInt(4),attrib.getInt(5),attrib.get(6).asInstanceOf[java.sql.Timestamp],attrib.get(7).asInstanceOf[java.sql.Timestamp],attrib.getInt(8),attrib.get(9).asInstanceOf[java.sql.Timestamp])).toDF()
          df3.createOrReplaceTempView("ODS_ITEMS")
          df3.show()
          println("Before start of SQL")
          val timeFmt = "yyyy-MM-dd HH:mm:ss.SSSS"
          val df4 = spark.sql("select ITEMID, BUSINESSID,SUBCATEGORYN, ITEMNUMBER,UNITPERPACKAGE,STATUS, ADDEDDATE,UPDATEDATE from ODS_ITEMS")
          
          val timeDiff = (functions.unix_timestamp(df4("UPDATEDATE"), timeFmt) - functions.unix_timestamp(df4("ADDEDDATE"), timeFmt))
          val df5 = df4.withColumn("Duration",timeDiff)
          def dateFiltering(x : Row) : Boolean = {
            val v = x.get(6).asInstanceOf[java.sql.Timestamp].getTime() - x.get(7).asInstanceOf[java.sql.Timestamp].getTime()
            return false
          }
          val df6 = df5.filter( "Duration < 7200")
          println("Total data after the filtering : " + df6.count())
          println("End of Sql execution")
          df6.repartition(1).write.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").save("/npd/processed/sabbirDir/" + dirComponents(dirComponents.length-1))
          Thread.sleep(2000)
          i += 1
          if (i > 1)
            break
        }
      }
    //}
  
  }*/
  def performJoin(df1 : org.apache.spark.sql.DataFrame , df2 : org.apache.spark.sql.DataFrame) : org.apache.spark.sql.DataFrame = {
          val dfF = df1.join(df2,$"df1itemid" === $"df2itemid")
          return dfF
  }
  def writeTohdfs(fileList : ListBuffer[(Path,Date)], fileName: String) : Unit = {
		val pw = new PrintWriter("/hdpdata/pysparkProject/dqMachine/src/main/python/dictionary/fileSource/" + fileName)
          	//fileList.foreach(pw.println)
		for(each <- fileList){
			pw.println(each._1)
		}
         	pw.close()  
		/*  
		val conf = new Configuration()
  		conf.set("fs.defaultFS","hdfs://lslhdprsnn01.npd.com:50070")
                val fs = FileSystem.get(conf)
                val output = fs.create(new Path("/npd/test/ODS/fileName"))

		val writer = new PrintWriter(output)
		try{
			for(each <- fileList){
				writer.write(each._1.toString)
				writer.write("\n")
			}
		}finally{
			writer.close()
			println("File Closed")
		}	*/
	}
  def main(args: Array[String]): Unit = {
    println("Start detecting todays file")
    //val tups_ = cleanupProcess("/npd/processed/sabbirDir/","2017-05-07T15:30:01",0)
    val arg : Array[String] = Array("hdfs:/npd/ODS/ODS_INPUTS/ODS_POSOUTLETITEMS/", "hdfs:/npd/ODS/ODS_INPUTS/ODS_POSITEMS/")
    for( each <- arg){
      val dataRdd = filesystemDiscovery(getPath(each))//FileSystem.get(sc.hadoopConfiguration).listStatus(new Path("hdfs:/npd")).map(x => x.getPath)
      val today = Calendar.getInstance.getTime()
      var yesterday = Calendar.getInstance()
      yesterday.add(Calendar.DATE, -1)
      val ytime = yesterday.getTime()
      val formatter = new java.text.SimpleDateFormat("dd-MM-yyyy")
      val today_ = formatter.parse(formatter.format(today))
      println(today_)
      val yesterday_ = formatter.parse(formatter.format(ytime))
      var fileNameDate: ListBuffer[(Path,Date)] = dataRdd.map(x => getDateTime(x)).filter(x => (!yesterday_.after(x._2) && !yesterday_.before(x._2)) ) 
      //fileNameDate.foreach(println)
      //fileNameDate = fileNameDate sortWith (_._2 > _._2)
      //val dtFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
      //fileNameDate = fileNameDate.map( x => (x._1,dateFormatting(dtFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(x._2),ZoneId.of("UTC")))))).map(x => (x._1,x._2(0),x._2(1),x._2(2), x._2(3),x._2(4)))
      fileNameDate.foreach(println)
      val fileN = each.split("/")
      if(fileNameDate.length != 0)
      	writeTohdfs(fileNameDate, fileN(fileN.length-1))
      else{
		println("Files are not available for " + fileN(fileN.length - 1) + " squooping issue" )
	}
       //fileNameDate = fileNameDate sortWith(sortingFunc(_,_))
      //if(tups_._2 == 1){
       // val pw = new PrintWriter("output/result.txt")
          //tups_._1.foreach(pw.println)
         //pw.close()    
      //}
      println("End of clean up process")
   }
    //dataRdd.foreach(println)  
  }
}
Main.main(Array(""))
System.exit(0)
