package homework5

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import java.util.ArrayList

import org.apache.spark.{SerializableWritable, SparkConf, SparkContext}

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object DistCp {

  def traverseDir(hdconf: Configuration, path: String, recursive: Boolean, filePaths: ArrayList[String],dirPaths:ArrayList[String]){
    val files = FileSystem.get(hdconf).listStatus(new Path(path))
    files.foreach { fStatus => {
      if (!fStatus.isDirectory ) {
        var path = fStatus.getPath.toString
        filePaths.add(fStatus.getPath.toString)

      }
      else if (fStatus.isDirectory) {

        var dir = fStatus.getPath.toString
        dirPaths.add(fStatus.getPath.toString)

        traverseDir(hdconf, fStatus.getPath.toString, recursive, filePaths,dirPaths)
      }

    }
    }
    filePaths
  }

  def createDirs(dirList:ArrayList[String],inputDir: Path, outputDir: Path, hdconf: Configuration): Unit = {

    val dirs = dirList.asScala
    var hdfs = FileSystem.get(hdconf)
    dirs.foreach { fStatus => {

      val targetDir = new Path(fStatus.replaceAll(inputDir.toString, outputDir.toString))

      if (!hdfs.exists(targetDir)) {
        hdfs.mkdirs(targetDir)
      }
    }
    }
  }

  def distcp(inputroot:Path,outputroot:Path,fileList:ArrayList[String],sparkContext: SparkContext,hdconf: Configuration,ignore_failures:Boolean,max_concurrence:Int):Unit={
    var stTuple = ListBuffer[(String, String)]()
    fileList.asScala.foreach(file => {
      stTuple += ((file.toString, file.toString.replaceAll(inputroot.toString, outputroot.toString)))

    })



    val stRdd = sparkContext.makeRDD(stTuple, max_concurrence)

    val rdd = stRdd.mapPartitions(source_target => {
      var result = ArrayBuffer[String]()
      while (source_target.hasNext) {
        val value = source_target.next()
        var source = value._1
        val target = value._2
        try {

          var config = new Configuration()
          var hdfs = FileSystem.get(config)

          val success = FileUtil.copy(hdfs, new Path(source), hdfs, new Path(target), false, config)
          if (success) {
            result += "copy " + source + " to " + target + " success"
          }
        } catch {
          case ex: Exception => {
            if (ignore_failures) {

              println( " copy failed "+source)
            } else {
              throw ex
            }
          }
        }

      }
      result.iterator
    }).foreach(println)

  }



  def main(args: Array[String]): Unit = {
    //val filePaths:List[String] = List()
    var ignore_failures =true
    var max_concurrence = 1
    var inputPath = "src/main/resources/files/"
    var outputPath = "src/main/resources/out/"

    val conf = new SparkConf()
    conf.setAppName("DistCp")
    conf.setMaster("local")
    //    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //    conf.set("spark.kryo.registrationRequired","true")

    val sparkContext = new SparkContext(conf)
    val confBroadcast = sparkContext.broadcast(new SerializableWritable(sparkContext.hadoopConfiguration))
    args.sliding(2, 2).toList.collect {
      case Array("-source", sc: String) => inputPath = sc
      case Array("-target", tg: String) => outputPath = tg
      case Array("-i", ignore: String) => ignore_failures = ignore.toBoolean
      case Array("-task", mc: String) => max_concurrence= mc.toInt

    }



    var filePaths:ArrayList[String] = new util.ArrayList[String]()
    var dirPaths:ArrayList[String] = new util.ArrayList[String]()
    @transient val configuration:Configuration = new Configuration()

    var scPath =  new Path(inputPath)
    var tgPath = new Path(outputPath)
    //var status = FileSystem.get(configuration).getFileStatus(scPath)
    var inputroot:Path = scPath.getParent
    var outputroot = tgPath



    //遍历目录
    traverseDir(configuration,inputPath,true,filePaths,dirPaths)
    //创建目录
    createDirs(dirPaths,inputroot,outputroot,configuration)
    //拷贝
    distcp(inputroot,outputroot,filePaths,sparkContext,confBroadcast.value.value,ignore_failures,max_concurrence)


  }


}
