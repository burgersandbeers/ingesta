//Librerías para desarrollo
import com.typesafe.config.ConfigFactory
import java.io.File

import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types.{StructType, StructField, StringType,IntegerType,DoubleType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveContext

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.log4j.PropertyConfigurator

import org.apache.hadoop.fs.Path
import java.io.InputStreamReader

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.io.IOException
import javax.security.auth.login.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext



object ingesta{
  // Variables globales

  val sc              = new SparkContext()
  var sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  val fs=FileSystem.get(sc.hadoopConfiguration)


  // =================== Funciones ===========================

  /**
  Obtener el valor de un parámetro dado un archivo de configuración. Formato String

 Inputs:
  config = sub-configuración que buscará en el archivo de configuración
  rutaConfigs = ruta en HDFS donde se encuentra el archivo de configuración de la aplicación
  parametro = parametro que se está buscando

Outputs:
  stringReturn = valor del parámetro solicitado en formato string

    **/
  def getParams(config: String, rutaConfigs: String, parametro: String): String = {

    var hadoopConfig = sc.hadoopConfiguration
    var fs = org.apache.hadoop.fs.FileSystem.get(hadoopConfig)
    var file = fs.open(new Path(rutaConfigs))
    val myConfigFile = new InputStreamReader(file)

    // var myConfigFile = new File(rutaConfigs)
    var fileConfig = ConfigFactory.parseReader(myConfigFile).getConfig(config)
    var stringReturn = "noFound"
    try{
      stringReturn = fileConfig.getString(parametro)
    }
    catch{ case e: Throwable => {
      println("No se pudo obtener el parámetro "+parametro)}
    }
    return stringReturn
  }


  /**
  Obtener el valor de un parámetro dado un archivo de configuración. Formato Lista

Inputs:
  config = String - sub-configuración que buscará en el archivo de configuración
  rutaConfigs = String - ruta en HDFS donde se encuentra el archivo de configuración de la aplicación
  parametro = String - parametro que se está buscando

Outputs:
  stringReturn = String - valor del parámetro solicitado en formato lista
    **/

  def getParamsList(config: String, rutaConfigs: String, parametro: String): java.util.List[String] = {
    var hadoopConfig = sc.hadoopConfiguration
    var fs = org.apache.hadoop.fs.FileSystem.get(hadoopConfig)
    var file = fs.open(new Path(rutaConfigs))
    val myConfigFile = new InputStreamReader(file)
    var fileConfig = ConfigFactory.parseReader(myConfigFile).getConfig(config)
    var stringReturn = fileConfig.getStringList(parametro)
    return stringReturn
  }

  /**
  Ver la cantidad de archivos que posee una ruta.

   Inputs:
    folder = String - ruta donde se verificará la cantidad de archivos
   Output:
    paths.length = Int - cantidad de archivos
    **/


  def folderEmpty(folder: String): Int = {
    var paths = fs.globStatus(new Path(folder+"/*") ).map(_.getPath)
    return paths.length
  }



  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)

    //Primer nivel de obtención de parámetros:
    //Ruta de configuraciones
    var rutaConfigs = args(0)
    // Nombre de la configuración
    var configName = args(1)

    log.info("=====================================================================")
    log.info("================== Inicio Obteniendo parametros =====================")
    log.info("=====================================================================")

    log.info("Las configuraciones son cargadas desde: "+rutaConfigs)
    log.info("Se cargan las configuraciones "+configName)


  


}
