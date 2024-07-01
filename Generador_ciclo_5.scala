import com.github.tototoshi.csv.*

import java.io.{File, FileWriter}
import java.util.Date
import scala.collection.mutable.ListBuffer

@main
def main():Unit = {
  val path = "C:\\intellij\\ProgramadorUtpl\\Proyecto_Integra\\100filas.csv"
  val reader: CSVReader = CSVReader.open(new File(path))
  val contentFile: List[Map[String, String]] = reader.allWithHeaders()
  val ruta: String = "C:\\intellij\\ProgramadorUtpl\\Proyecto_Integra\\Person.csv"
  val ruta2: String = "C:\\intellij\\ProgramadorUtpl\\Proyecto_Integra\\Nacionalidad.csv"
  val ruta3: String = "C:\\intellij\\ProgramadorUtpl\\Proyecto_Integra\\Arma.csv"
  val ruta4: String = "C:\\intellij\\ProgramadorUtpl\\Proyecto_Integra\\Delito.csv"
  val ruta5: String = "C:\\intellij\\ProgramadorUtpl\\Proyecto_Integra\\Detencion.csv"
  val ruta6: String = "C:\\intellij\\ProgramadorUtpl\\Proyecto_Integra\\Distrito.csv"
  val escribri: FileWriter = new FileWriter(ruta)
  val escribri_2: FileWriter = new FileWriter(ruta2)
  val escribri_arma: FileWriter = new FileWriter(ruta3)
  val escribri_Delito: FileWriter = new FileWriter(ruta4)
  val escribri_detencion: FileWriter = new FileWriter(ruta5)
  val escribri_distrito: FileWriter = new FileWriter(ruta6)
  var nacionalidadEquals:ListBuffer[String]=ListBuffer[String]()
  var aramasEquals:ListBuffer[String]=ListBuffer[String]()
  var personEquals:ListBuffer[String]=ListBuffer[String]()
  var delitoEquals:ListBuffer[String]=ListBuffer[String]()
  var distritoEquals:ListBuffer[String]=ListBuffer[String]()

  def Provincia(data: List[Map[String, String]]): Unit = {
    val intermediario: List[(Int,String)] = data.map(
      hasMap => (
        (hasMap("codigo_provincia")).toInt,
        (hasMap("nombre_provincia")))
    ).distinct.sorted

    intermediario.foreach { tupla =>
      val formatIn: String = s"INSERT INTO provincia(codigo_provincia,nombre_provincia) VALUES(${tupla._1},'${tupla._2}');"
      println(tupla._2)
      escribri.write(formatIn + " " + "\n")
    }
    escribri.close()
  }

  def Canton(data: List[Map[String, String]]): Unit = {
    val intermediario: List[(Int, String,Int)] = data.map(
      hasMap => (
        hasMap("codigo_canton").toInt,
        hasMap("nombre_canton"),
        hasMap("codigo_provincia").toInt)
    ).distinct.sorted

    intermediario.foreach { tupla =>
      val formatIn: String = s"INSERT INTO canton(codigo_canton,nombre_canton,codigo_provincia) VALUES(${tupla._1},'${tupla._2}',${tupla._3});"
      escribri.write(formatIn + " " + "\n")
    }
    escribri.close()
  }

  def Arma(data: List[Map[String, String]]): Unit = {
    val listaArma:List[String]=List[String]("NINGUNA","ARMAS DE FUEGO","ARMAS BLANCAS (OBJETO CORTANTE O PUNZANTE)")
    val intermediario: List[(String, String)] = data.map(
      hasMap => (
        hasMap("arma"),
        hasMap("tipo_arma"))
    ).distinct.sorted
    intermediario.zipWithIndex.foreach { case (valor, indice) =>
      var id: Int = 0;
      if (valor._2.equalsIgnoreCase("NINGUNA")) {
        id = 1
      } else if (valor._2.equalsIgnoreCase("ARMAS DE FUEGO")) {
        id = 2
      } else {
        id = 3
      }
      aramasEquals+=valor._1
      val formatIn: String = s"INSERT INTO armas(nombre,arma_id,tipo_arma_id) VALUES('${valor._1}',${indice+1},${id});"
      escribri_arma.write(formatIn + " " + "\n")

    }
    escribri_arma.close()
  }

  def tipo_arma(data: List[Map[String, String]]): Unit = {
    val tipoArma: List[String] = data.map { hasMap =>
      hasMap("tipo_arma")
    }.distinct

    tipoArma.zipWithIndex.foreach { case (valor, indice) =>
      val formatIn: String = s"INSERT INTO tipo_arma(tipo_arma_id, nombre) VALUES(${indice + 1},'${valor}');"
      escribri.write(formatIn + " " + "\n")
    }
    escribri.close()
  }


  def Delito(data: List[Map[String, String]]): Unit = {
    val intermediario: List[(String,String,String,String,String,String,String,String,String)] = data.map(
      hasMap => (
        hasMap("presunta_flagrancia"),
        hasMap("presunta_infraccion"),
        hasMap("presunta_subinfraccion"),
        hasMap("presunta_modalidad"),
        hasMap("codigo_iccs"),
        hasMap("movilizacion"),
        hasMap("tipo"),
        hasMap("condicion"),
        hasMap("codigo_distrito"))
    ).distinct.sorted
    intermediario.zipWithIndex.foreach { case (tuple, indice) =>
      delitoEquals+=tuple._5
      val nuevaList:ListBuffer[String]=distritoEquals.distinct
      val posicion_distrito = nuevaList.indexOf(tuple._9)
      val formatIn: String = s"INSERT INTO delito(delito_id,presunta_flagrancia,presunta_infraccion,presunta_subinfraccion,presunta_modalidad,codigo_iccs,movilizacion,tipo,condicion,distrito_id) VALUES(${indice+1},'${tuple._1}','${tuple._2}','${tuple._3}','${tuple._4}','${tuple._5}','${tuple._6}','${tuple._7}','${tuple._8}',${posicion_distrito+1});"
      escribri_Delito.write(formatIn + " " + "\n")
    }
    escribri_Delito.close()
  }





  def nacionalidad(data: List[Map[String, String]]): Unit = {
    val nacionalidad: List[String] = data.map { hasMap =>
      hasMap("nacionalidad")
    }.distinct.sorted
    nacionalidad.zipWithIndex.foreach { case (valor, indice) =>
      val formatIn: String = s"INSERT INTO nacionalidad(nacionalidad_id, nombre) VALUES(${indice + 1},'${valor}');"
      nacionalidadEquals += valor
      escribri_2.write(formatIn + " " + "\n")
    }
    escribri_2.close()
  }

  def Person(data: List[Map[String, String]]): Unit = {
    var formatIn: String = "NULL";
    var listaEquals: ListBuffer[String] = ListBuffer[String]()
    var listaWrite: ListBuffer[String] = ListBuffer[String]()
    val intermediario: List[(String, String, Int, String, String,String, String,String)] = data.map(
      hasMap => (
        hasMap("estado_civil"),
        hasMap("estatus_migratorio"),
        hasMap("edad").toInt,
        hasMap("sexo"),
        hasMap("autoidentificacion_etnica"),
        hasMap("nivel_de_instruccion"),
        hasMap("genero"),
        hasMap("nacionalidad"))
    )
    intermediario.zipWithIndex.foreach { case (tuple, indice) =>
      formatIn = s"INSERT INTO person(estado_civil,estatus_migratorio,edad,sexo,autoidentificacion_etnica,nivel_de_instruccion,genero) VALUES('${tuple._1}','${tuple._2}',${tuple._3},'${tuple._4}','${tuple._5}','${tuple._6}','${tuple._7}')"
      if (!listaEquals.contains(formatIn)) {
        personEquals+=s"INSERT INTO person(estado_civil,estatus_migratorio,edad,sexo,autoidentificacion_etnica,nivel_de_instruccion,genero) VALUES('${tuple._1}','${tuple._2}',${tuple._3},'${tuple._4}','${tuple._5}','${tuple._6}','${tuple._7}')"
        listaEquals += formatIn
        val posicion = nacionalidadEquals.indexOf(tuple._8)
        listaWrite += s"INSERT INTO person(persona_id,estado_civil,estatus_migratorio,edad,sexo,autoidentificacion_etnica,nivel_de_instruccion,nacionalidad_id,genero) VALUES(${indice + 1},'${tuple._1}','${tuple._2}',${tuple._3},'${tuple._4}','${tuple._5}','${tuple._6}',${posicion+1},'${tuple._7}');"
      }
    }
    listaWrite.foreach(insert => escribri.write(insert + " " + "\n"))
    escribri.close()
  }


  def Detencion(data: List[Map[String, String]]): Unit = {
    var formatIn: String = "NULL";
    val intermediario: List[(String, String, String, String,Int, String, String, Int, String, String,String, String, String,String,String)] = data.map(
      hasMap => (
        hasMap("fecha_detencion_aprehension"),
        hasMap("hora_detencion_aprehension"),
        hasMap("lugar"),
        hasMap("tipo_lugar"),
        hasMap("numero_detenciones").toInt,
        hasMap("estado_civil"),
        hasMap("estatus_migratorio"),
        hasMap("edad").toInt,
        hasMap("sexo"),
        hasMap("autoidentificacion_etnica"),
        hasMap("nivel_de_instruccion"),
        hasMap("genero"),
        hasMap("nacionalidad"),
        hasMap("codigo_iccs"),
        hasMap("arma"))
    )
    intermediario.zipWithIndex.foreach { case (tuple, indice) =>
      var string_nuevo = tuple._1.replaceAll("T", " ").replaceAll("000", " ").toCharArray.filter(_!='.').mkString
      var string_nuevo_2 = tuple._2.replaceAll("T", " ").replaceAll("000", " ").toCharArray.filter(_!='.').mkString
      val posicion_delito_id = delitoEquals.indexOf(tuple._14)
      val posicion_persona_id = personEquals.indexOf(s"INSERT INTO person(estado_civil,estatus_migratorio,edad,sexo,autoidentificacion_etnica,nivel_de_instruccion,genero) VALUES('${tuple._6}','${tuple._7}',${tuple._8},'${tuple._9}','${tuple._10}','${tuple._11}','${tuple._12}')")
      val posicion_arma_id = aramasEquals.indexOf(tuple._15)
      formatIn = s"INSERT INTO detencion(detencion_id,fecha_detencion_aprehension,hora_detencion_aprehension,lugar,tipo_lugar,delito_id,persona_id,arma_id,numero_detenciones) VALUES(${indice+1}, TIMESTAMP '${string_nuevo}', TIMESTAMP '${string_nuevo_2}','${tuple._3}','${tuple._4}',${posicion_delito_id+1},${posicion_persona_id+1},${posicion_arma_id+1},${tuple._5});"
      escribri_detencion.write(formatIn + " " + "\n")
    }
    escribri_detencion.close()
  }




  def parroquia(data: List[Map[String, String]]): Unit = {
    val parroquia: List[(Int, String, String)] = data.map { hasMap =>
      (
        hasMap("codigo_parroquia").toInt,
        hasMap("nombre_parroquia"),
        hasMap("codigo_canton")
      )
    }
    val parroquiaUnicaYOrdenada = parroquia.distinct.sortBy(_._1)
    parroquiaUnicaYOrdenada.foreach { tupla =>
      val formatIn: String = s"INSERT INTO parroquia(codigo_parroquia, nombre_parroquia, codigo_canton) VALUES(${tupla._1}, '${tupla._2}', ${tupla._3});"
      escribri.write(formatIn + " " + "\n")
    }
    escribri.close()
  }




  def distrito(data: List[Map[String, String]]): Unit = {
    val distrito: List[(String, String, String, String, String, String, String, String, Int)] = data.map { hasMap =>
      (
        hasMap("codigo_distrito"),
        hasMap("nombre_distrito"),
        hasMap("nombre_zona"),
        hasMap("nombre_subzona"),
        hasMap("codigo_subcircuito"),
        hasMap("nombre_subcircuito"),
        hasMap("codigo_circuito"),
        hasMap("nombre_circuito"),
        hasMap("codigo_parroquia").toInt
      )
    }
    val distritoOrdenadoYUnico = distrito.distinct.sortBy(_._1)
    distritoOrdenadoYUnico.zipWithIndex.foreach { case (tupla, indice) =>
      distritoEquals+=tupla._1
      val formatIn: String = s"INSERT INTO distrito(distrito_id,codigo_distrito, nombre_distrito, nombre_zona, nombre_subzona, codigo_subcircuito, nombre_subcircuito, " +
        s"codigo_circuito, nombre_circuito, codigo_parroquia) " +
        s"VALUES(${indice+1},'${tupla._1}', '${tupla._2}', '${tupla._3}', '${tupla._4}', '${tupla._5}', '${tupla._6}', " +
        s"'${tupla._7}', '${tupla._8}', ${tupla._9});"
      escribri_distrito.write(formatIn + " " + "\n")
    }
    escribri_distrito.close()
  }











  //Provincia(contentFile)
  //Canton(contentFile)
  //tipo_arma(contentFile)
  //parroquia(contentFile)



  //Arma(contentFile)
  //distrito(contentFile)
  //Delito(contentFile)
  //nacionalidad(contentFile)
  //Person(contentFile)
  //Detencion(contentFile)


  //////////////////////////////////////////////////////////

}