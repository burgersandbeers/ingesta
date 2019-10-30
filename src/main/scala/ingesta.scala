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


    var logLevelParam = getParams(configName,rutaConfigs,"LOG_LEVEL")
    var rutaArchivos = getParams(configName,rutaConfigs,"RUTA_ARCHIVOS")
    var dataBase = getParams(configName,rutaConfigs,"BASE_DE_DATOS")
    var destTable = getParams(configName,rutaConfigs,"TABLA_DESTINO")
    var zonaHoraria = getParams(configName,rutaConfigs,"ZONA_HORARIA_IMPALA")

    // Valor puede ser MANUAL o AUTOMATICO
    var flagProcesamiento = getParams(configName,rutaConfigs,"TIPO_PROCESAMIENTO")

    // cuando es AUTOMATICO requiere estos parámetros:
    var offsetHora = getParams(configName,rutaConfigs,"OFFSET_HORA")
    var flagSetZonaHorariaSpark = getParams(configName,rutaConfigs,"CAMBIAR_ZONA_HORARIA_SPARK")
    var offsetDateSpark = getParams(configName,rutaConfigs,"CAMBIO_ZONA_HORARIA_SPARK")


    // cuando es MANUAL requiere estos parámetros:
    var dayProcess= getParams(configName,rutaConfigs,"FECHA_A_PROCESAR")
    var revisarOtroDia= getParams(configName,rutaConfigs,"VER_SIGUIENTE_DIA")
    var horaIni = getParams(configName,rutaConfigs,"HORA_INI")
    var horaFin = getParams(configName,rutaConfigs,"HORA_FIN")
    var flagRutaEspecifica = getParams(configName,rutaConfigs,"PROCESAR_RUTA_ESPECIFICA")


    println("Valor Parametros: ------------------------------------------")

    println("Parámetros para sistema en general:")

    println("LOG_LEVEL: "+logLevelParam)
    println("RUTA_ARCHIVOS: "+rutaArchivos)
    println("BASE_DE_DATOS: "+dataBase)
    println("TABLA_DESTINO: "+destTable)
    println("ZONA_HORARIA_IMPALA: "+zonaHoraria)

    println(" ")
    println(" ")
    println("Tipo de procesamiento:")
    println("TIPO_PROCESAMIENTO: "+flagProcesamiento)

    println(" ")
    println(" ")
    println("Variables utilizadas en procesamiento automático:")
    println("OFFSET_HORA: "+offsetHora)
    println("CAMBIAR_ZONA_HORARIA_SPARK: "+flagSetZonaHorariaSpark)
    println("CAMBIO_ZONA_HORARIA_SPARK: "+offsetDateSpark)



    println(" ")
    println(" ")
    println("Variables utilizadas en procesamiento manual:")
    println("FECHA_A_PROCESAR: "+dayProcess)
    println("HORA_INI: "+horaIni)
    println("HORA_FIN: "+horaFin)
    println("PROCESAR_RUTA_ESPECIFICA: "+flagRutaEspecifica)

    println("Fin valor parámetros -----------------------------------------------------------")

    log.info("==================================================================")
    log.info("================== Fin Obteniendo parametros =====================")
    log.info("==================================================================")




    // Configuraciones globales:


    // Esquema de LECTURA de las tablas:
   

    // Formato de la fecha
    var dateFormat = new SimpleDateFormat( "yyyy-MM-dd" )
    var dateFormatRuta = new SimpleDateFormat("yyyy-MM-dd-HH")
    var calendar = Calendar.getInstance()


    // Obtención de variables internas
    var tempDB = "temporal_3g"
    var tempDBDateHour = "temporal_3g_2"
    var rutaDB = dataBase+"."+destTable


    // Inserción general:

    // Esto irá dentro de la Query Impala y será los campos generales que se selecciorán durante los 3 métodos de ejecucíón
    // propuestos.
 


    log.info("Iniciando cambio de parámetros considerando las variables anteriores")
    log.info("Tipo de procesamiento: "+flagProcesamiento)

    if (flagProcesamiento=="AUTOMATICO"){

      // ------------ Cambio de Dia y Hora según parámetros otorgados
      log.info("Se procede a realizar cambios de fecha-hora")

      if (flagSetZonaHorariaSpark=="true"){
        log.info("Se procede a cambiar la zona horaria del sistema spark para el cálculo de la hora")
        dateFormat.setTimeZone(TimeZone.getTimeZone(offsetDateSpark))
      }

      log.info("Se procede a cambiar la hora del sistema spark para el cálculo de hora")
      calendar.add(Calendar.HOUR_OF_DAY,offsetHora.toInt)
      // ---------------- Fin cambios de Hora



      // Obtención de variables para procesamiento: Hora y Día
      var dayProcesamiento = dateFormat.format(calendar.getTime())
      var hourProcesamientoGet = calendar.get(Calendar.HOUR_OF_DAY)
      var hourProcesamiento="%02d".format(hourProcesamientoGet)


      // Generación ruta donde están los archivos a procesar.
      var rutaInstancia = rutaArchivos+"/"+dayProcesamiento+"-"+hourProcesamiento
      log.info("La ruta de donde se leeran los archivos es: "+rutaInstancia)


      // Revisión de cantidad de archivos en la ruta (para evitar creación de carpetas hiveStage)
      var cantidadArchivos = folderEmpty(rutaInstancia)

      if (cantidadArchivos != 0){
        log.info("Creando RDD en base a ruta: "+ rutaInstancia)
        try{

          // Lectura de registros en HDFS
          // var rdd = sc.textFile(rutaInstancia).filter( x=> x contains "<Row>").map(x =>x.replace("<Row>","").replace("</Row>","").replace("<v>","").replace("</v>","|").replace("<v/>","|")).map(x=> Row.fromSeq(x.split("\\|"))).filter(x => x.length == schema.length)
          var rdd = sc.textFile(rutaInstancia).filter( x=> x contains "<Row>").map(x =>x.replace("<Row>","").replace("</Row>","").replace("<v>","").replace("</v>","<SEPARADOR>").replace("<v/>","<SEPARADOR>")).map(x=> Row.fromSeq(x.split("<SEPARADOR>"))).filter(x => x.length == schema.length)

          // Creaciónd de DF con toda la data
          log.info("Crear DF a partir de RDD anterior")
          var df = sqlContext.createDataFrame(rdd,schema)
          df.registerTempTable(tempDB)

          // Preparación Data 3G
          var querySelect = "SELECT *,to_date(from_utc_timestamp(cast(unix_timestamp(cast(date_add('1900-01-01',cast(absolutetime as int)-2)as timestamp)) + (absolutetime - cast(absolutetime as int))*86400 as timestamp),'"+zonaHoraria+"')) as dia, hour(from_utc_timestamp(cast(unix_timestamp(cast(date_add('1900-01-01',cast(absolutetime as int)-2)as timestamp)) + (absolutetime - cast(absolutetime as int))*86400 as timestamp),'"+zonaHoraria+"' )) as hora FROM "+tempDB
          log.info("Obteniendo registros de :"+tempDB)
          var dfDateHour = sqlContext.sql(querySelect)
          dfDateHour.registerTempTable(tempDBDateHour)


          // Inserción Data 3g del día a procesar --- Acá se inserta el dayProcess and hourProcess
          sqlContext.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

          // Se agrega el día y hora de procesamiento que está buscando AUTOMATICAMENTE el proceso
          var camposInsert2= "'"+dayProcesamiento.toString+"' as ingest_date, "+hourProcesamiento.toInt+" as ingest_hour, dia, hora "
          var query = "INSERT INTO TABLE "+rutaDB+" PARTITION(dia,hora) SELECT "+camposInsert+camposInsert2+" FROM "+tempDBDateHour+ " DISTRIBUTE BY (dia,hora)"
          // var query = "INSERT INTO TABLE "+rutaDB+" PARTITION(dia,hora) SELECT "+camposInsert+camposInsert2+" FROM "+tempDBDateHour
          log.info("Insertando registros a "+rutaDB+": "+query)
          sqlContext.sql(query)
          log.info("Se han insertado los registros a la tabla "+rutaDB)

        } catch {
          case e: Throwable => {
            log.error("Error al guardar registros en tabla final. Error="+e.toString)
          }
        }
      }
      else{
        log.error("No hay registros asociados a esa ruta. Ruta: "+rutaInstancia)
      }

    }
    else if (flagProcesamiento=="MANUAL"){

      // Caso para reprocesar todo día-hora específica
      if (flagRutaEspecifica=="true"){
        log.info("Obteniendo ruta espécifica otorgada por linea de comandos")


        try {
          // Obtención ruta específica
          var rutaManual = args(2)
          log.info("Se obtiene ruta específica. Ruta= "+rutaManual)
          var cantidadArchivos = folderEmpty(rutaManual)

          if (cantidadArchivos != 0){
            // Obtención fecha parseada:
            var dateDeRuta = rutaManual.split("/").last
            calendar.setTime(dateFormatRuta.parse(dateDeRuta))

            //Obtención de fecha y hora a partir de ruta especificada
            var dayProcesamiento=dateFormat.format(calendar.getTime())
            var hourProcesamiento=calendar.get(Calendar.HOUR_OF_DAY)


            // Lecutra del RDD
            // var rdd = sc.textFile(rutaManual).filter( x=> x contains "<Row>").map(x =>x.replace("<Row>","").replace("</Row>","").replace("<v>","").replace("</v>","|").replace("<v/>","|")).map(x=> Row.fromSeq(x.split("\\|")))
            var rdd = sc.textFile(rutaManual).filter( x=> x contains "<Row>").map(x =>x.replace("<Row>","").replace("</Row>","").replace("<v>","").replace("</v>","<SEPARADOR>").replace("<v/>","<SEPARADOR>")).map(x=> Row.fromSeq(x.split("<SEPARADOR>")))

            // Creaciónd de DF con toda la data
            log.info("Crear DF a partir de RDD anterior")
            var df = sqlContext.createDataFrame(rdd,schema)
            df.registerTempTable(tempDB)


            // Preparación Data 3G
            var querySelect = "SELECT *,to_date(from_utc_timestamp(cast(unix_timestamp(cast(date_add('1900-01-01',cast(absolutetime as int)-2)as timestamp)) + (absolutetime - cast(absolutetime as int))*86400 as timestamp),'"+zonaHoraria+"')) as dia, hour(from_utc_timestamp(cast(unix_timestamp(cast(date_add('1900-01-01',cast(absolutetime as int)-2)as timestamp)) + (absolutetime - cast(absolutetime as int))*86400 as timestamp),'"+zonaHoraria+"' )) as hora FROM "+tempDB
            log.info("Obteniendo registros de :"+tempDB)
            var dfDateHour = sqlContext.sql(querySelect)
            dfDateHour.registerTempTable(tempDBDateHour)


            // Inserción Data 3g del día a procesar --- Acá se inserta el dayProcess and hourProcess
            sqlContext.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

            var camposInsert2 = "'"+dayProcesamiento.toString+"' as ingest_date, "+hourProcesamiento.toInt+" as ingest_hour, dia, hora "
            var query = "INSERT INTO TABLE "+rutaDB+" PARTITION(dia,hora) SELECT "+camposInsert+camposInsert2+" FROM "+tempDBDateHour+ " DISTRIBUTE BY (dia,hora)"
            log.info("Insertando registros a "+rutaDB+": "+query)
            sqlContext.sql(query)
            log.info("Se han insertado los registros a la tabla "+rutaDB)
          }

          else{
            log.error("No hay registros asociados a esa ruta. Ruta: "+rutaManual)
          }

        } catch {
          case e: Throwable => {
            log.info("No fue posible procesar 3G. Error="+e.toString)
          }
        }

      }


      // Caso para reprocesar todo día-hora específica
      else if (flagRutaEspecifica=="false"){

        log.info("Obtención de fecha del archivo de configuración.")

        var cal = Calendar.getInstance()
        // Se cambia el calendario del sistema del día a procesar.
        cal.setTime(dateFormat.parse(dayProcess))

        //Se agrega 1 día al calendario de sistema para la obtención del siguiente día:
        cal.add(Calendar.DATE,1)
        var dayProcessNextDay = dateFormat.format(cal.getTime())


        //dayProcess -> Dia a procesar
        //dayProcessNextDay -> Dia siguiente

        log.info("Dia a procesar: "+dayProcess)
        log.info("Dia siguiente donde se buscarán registros del día a procesar: "+dayProcessNextDay)



        val listDays = List(dayProcess,dayProcessNextDay)


        // Looop para buscar los archivos correspondientes a la hora ini y hora fin
        // Si hora ini == hora fin
        for (day <- listDays){
          for (i <- horaIni.toInt to horaFin.toInt){
            try {

              var hour="%02d".format(i)
              var rutaArchivoInstancia = rutaArchivos+"/"+day+"-"+hour


              log.info("Crear RDD de datos en base a esta ruta de archivos "+ rutaArchivoInstancia)

              // Lectura de registros que están en HDFS
              // var rdd = sc.textFile(rutaArchivoInstancia).filter( x=> x contains "<Row>").map(x =>x.replace("<Row>","").replace("</Row>","").replace("<v>","").replace("</v>","|").replace("<v/>","|")).map(x=> Row.fromSeq(x.split("\\|"))).filter(x => x.length == schema.length)
              var rdd = sc.textFile(rutaArchivoInstancia).filter( x=> x contains "<Row>").map(x =>x.replace("<Row>","").replace("</Row>","").replace("<v>","").replace("</v>","<SEPARADOR>").replace("<v/>","<SEPARADOR>")).map(x=> Row.fromSeq(x.split("<SEPARADOR>"))).filter(x => x.length == schema.length)


              // Creaciónd de DF con toda la data
              log.info("Crear DF a partir de RDD anterior")
              var df = sqlContext.createDataFrame(rdd,schema)
              df.registerTempTable(tempDB)

              // Preparación Data 3G
              var querySelect = "SELECT *,to_date(from_utc_timestamp(cast(unix_timestamp(cast(date_add('1900-01-01',cast(absolutetime as int)-2)as timestamp)) + (absolutetime - cast(absolutetime as int))*86400 as timestamp),'"+zonaHoraria+"')) as dia, hour(from_utc_timestamp(cast(unix_timestamp(cast(date_add('1900-01-01',cast(absolutetime as int)-2)as timestamp)) + (absolutetime - cast(absolutetime as int))*86400 as timestamp),'"+zonaHoraria+"' )) as hora FROM "+tempDB
              log.info("Obteniendo registros de :"+tempDB)
              var dfDateHour = sqlContext.sql(querySelect)
              dfDateHour.registerTempTable(tempDBDateHour)


              // Inserción Data 3G del día a procesar
              sqlContext.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

              var camposInsert2 = "'"+day.toString+"' as ingest_date, "+hour.toInt+" as ingest_hour, dia, hora "

              var query = "INSERT INTO TABLE "+rutaDB+" PARTITION(dia,hora) SELECT "+camposInsert+camposInsert2+" FROM "+tempDBDateHour+" WHERE dia='"+dayProcess+"'"
              log.info("Insertando registros a "+rutaDB+": "+query)
              sqlContext.sql(query)

            } catch {
              case e: Throwable => {
                log.error("Error al guardar registros en tabla temporal. Error="+e.toString)
              }
            }

          } // End for horas
        } // End for dias

      }

      else {
        log.error("El argumento PROCESAR_RUTA_ESPECIFICA no  es válido, los valores deben ser true o false. Actual="+flagRutaEspecifica)
      }

    }
    else{
      log.error("El tipo de procesamiento no está permitido. Se debe usar AUTOMATICO o MANUAL. Usado actualmente: "+ flagProcesamiento)
    }

  }


}
