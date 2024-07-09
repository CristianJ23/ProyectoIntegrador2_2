import com.github.tototoshi.csv.*

import java.io.{File, FileWriter, PrintWriter}
import scala.util.Try

@main
def main(): Unit = {
  val path = "src/resources/data.csv"
  val pathComplementarios = "C:\\cuarto_semestre\\proyectoIntegrador\\datosGenerados\\dataComplementaria.csv\\data_complementaria.csv"
  val readerCompl: CSVReader = CSVReader.open(new File(pathComplementarios))
  val contentFileComplementarios: List[Map[String, String]] = readerCompl.allWithHeaders()
  val reader: CSVReader = CSVReader.open(new File(path))
  val contentFile: List[Map[String, String]] = reader.allWithHeaders()
  val ruta: String = "src/resources/parroquia.csv"
  val rutaPersona: String = "C:\\cuarto_semestre\\proyectoIntegrador\\datosGenerados\\PersonasSql.csv"
  val ruta2: String = "src/resources/Nacionalidad.csv"
  val ruta3: String = "src/resources/Arma.csv"
  val ruta4: String = "C:\\cuarto_semestre\\proyectoIntegrador\\datosGenerados\\delitosSql.csv"
  val ruta5: String = "C:\\cuarto_semestre\\proyectoIntegrador\\datosGenerados\\Detencion.csv"
  val rutaComp: String = "src/resources/Comoplementarios.csv"
  val ruta6: String = "src/resources/Distrito.csv"
  val escribri: FileWriter = new FileWriter(ruta)
  val escribri_2: FileWriter = new FileWriter(ruta2)
  val escribri_arma: FileWriter = new FileWriter(ruta3)
  val escribri_Delito: FileWriter = new FileWriter(ruta4)
  val escribri_detencion: FileWriter = new FileWriter(ruta5)
  val escribi_complementarios: FileWriter = new FileWriter(rutaComp)
  val escribri_distrito: FileWriter = new FileWriter(ruta6)
  val escribi_persona: FileWriter = new FileWriter(rutaPersona)


  // -------------------------------------***************************** COMPLEMENTARIOS ***********************************---------------------------
  def complementarios(data: List[Map[String, String]]): Unit = {
    val intermediario: List[(String, Int, Int, Int, Int, Int, Int, Int, Int, Int)] = data.map { hashMap =>
      def safeToInt(key: String): Int = {
        val str = hashMap.getOrElse(key, "")
        if (str.nonEmpty) str.toInt else -1
      }

      (
        hashMap("codigo_provincia"),
        safeToInt("edad"),
        safeToInt("sexo"),
        safeToInt("sabe_leer"),
        safeToInt("mes"),
        safeToInt("pobreza"),
        safeToInt("pobreza_extrema"),
        safeToInt("empleo"),
        safeToInt("titulo_por_estudios"),
        safeToInt("anio")
      )
    }.distinct

    intermediario.zipWithIndex.foreach { case (tupla, indice) =>
      val formatIn: String =
        s"INSERT INTO complementarios(complementarios_id, codigo_provincia, edad, sexo, sabe_leer, mes, pobreza, pobreza_extrema, empleo, titulo_por_estudios, anio) " +
          s"VALUES(${indice + 1}, '${tupla._1}', ${tupla._2}, ${tupla._3}, ${tupla._4}, ${tupla._5}, ${tupla._6}, ${tupla._7}, ${tupla._8}, ${tupla._9}, ${tupla._10});"
      escribi_complementarios.write(formatIn + " " + "\n")
    }
    escribi_complementarios.close()
  }

  //complementarios(contentFileComplementarios)
  // -------------------------------------***************************** PROVINCIA ***********************************---------------------------
  def Provincia(data: List[Map[String, String]]): Unit = {
    val intermediario: List[(String, String)] = data
      .filter(x => x("codigo_provincia").nonEmpty)
      .map(
        hasMap => (
          (hasMap("codigo_provincia")),
          (hasMap("nombre_provincia")))
      )

    val provinciaUnica = intermediario.distinctBy(_._1).sorted
    provinciaUnica.foreach { tupla =>
      val formatIn: String = s"INSERT INTO provincia(codigo_provincia,nombre_provincia) VALUES('${tupla._1}','${tupla._2}');"
      escribri.write(formatIn + " " + "\n")
    }
    escribri.close()
  }
  //Provincia(contentFile)

  // -------------------------------------***************************** CANTON ***********************************---------------------------
  def Canton(data: List[Map[String, String]]): Unit = {
    val intermediario: List[(Int, String, String)] = data
      .filter(_("codigo_canton").nonEmpty)
      .map { hasMap =>
        (
          hasMap("codigo_canton").trim.toInt,
          hasMap("nombre_canton"),
          hasMap("codigo_provincia")
        )
      }

    val cvsCanton = new File("src/main/resorces2/Canton.csv")
    val csvWriter = CSVWriter.open(cvsCanton)
    csvWriter.writeRow(List("codigo_canton", "nombre_canton", "codigo_provincia"))

    val cantonUnico = intermediario.distinctBy(_._1).sorted
    cantonUnico.foreach { tupla =>
      val formatIn: String = s"INSERT INTO canton(codigo_canton,nombre_canton,codigo_provincia) VALUES(${tupla._1},'${tupla._2}',${tupla._3});"
      escribri.write(formatIn + "\n")
      csvWriter.writeRow(List(tupla._1, tupla._2, tupla._3))
    }
    escribri.close()
    csvWriter.close()
  }
  //Canton(contentFile)

  // -------------------------------------***************************** PARROQUIA ***********************************---------------------------
  def parroquia(data: List[Map[String, String]]): Unit = {
    val parroquia: List[(Option[Int], String, Option[Int])] = data.flatMap { hashMap =>
      val codigoParroquiaOpt = hashMap.get("codigo_parroquia").flatMap(str => Try(str.toInt).toOption)
      val nombreParroquia = hashMap.getOrElse("nombre_parroquia", "")
      val codigoCantonOpt = hashMap.get("codigo_canton").flatMap(str => Try(str.toInt).toOption)
      codigoParroquiaOpt.map(cp => (Some(cp), nombreParroquia, codigoCantonOpt))
    }

    val parroquiaUnicaYOrdenada = parroquia.distinctBy(_._1).sorted

    parroquiaUnicaYOrdenada.foreach { tupla =>
      val codigoParroquiaStr = tupla._1.map(_.toString).getOrElse("NULL")
      val codigoCantonStr = tupla._3.map(_.toString).getOrElse("NULL")
      val formatIn: String = s"INSERT INTO parroquia(codigo_parroquia, nombre_parroquia, codigo_canton) VALUES ($codigoParroquiaStr, '${tupla._2}', $codigoCantonStr);"
      escribri.write(formatIn + " " + "\n")
    }

    escribri.close()
  }
  //parroquia(contentFile)

  // -------------------------------------***************************** DISTRITO ***********************************---------------------------
  def distrito(data: List[Map[String, String]]): Unit = {
    // Crear una lista de tuplas con los datos, permitiendo valores vacíos
    val distrito: List[(String, String, String, String, String, String, String, String, Option[Int])] = data.map { hasMap =>
      (
        hasMap.getOrElse("codigo_distrito", ""),
        hasMap.getOrElse("nombre_distrito", ""),
        hasMap.getOrElse("nombre_zona", ""),
        hasMap.getOrElse("nombre_subzona", ""),
        hasMap.getOrElse("codigo_subcircuito", ""),
        hasMap.getOrElse("nombre_subcircuito", ""),
        hasMap.getOrElse("codigo_circuito", ""),
        hasMap.getOrElse("nombre_circuito", ""),
        if (hasMap.getOrElse("codigo_parroquia", "").nonEmpty) Some(hasMap("codigo_parroquia").toInt) else None,
      )
    }.distinct.sorted

    // ------------------------ CSV SECUNDARIO DISTRITO ---------------------------------
    val cvsDistrito = new File("src/main/resorces2/Distrito.csv")
    val csvWriter = CSVWriter.open(cvsDistrito)
    try {
      csvWriter.writeRow(List("distrito_id", "codigo_distrito", "nombre_distrito", "nombre_zona", "nombre_subzona", "codigo_subcircuito", "nombre_subcircuito", "codigo_circuito", "nombre_circuito", "codigo_parroquia"))

      distrito.zipWithIndex.foreach { case (tupla, indice) =>
        val codigoParroquia = tupla._9.map(_.toString).getOrElse("''")
        val formatIn: String = s"INSERT INTO distrito(distrito_id,codigo_distrito, nombre_distrito, nombre_zona, nombre_subzona, codigo_subcircuito, nombre_subcircuito, " +
          s"codigo_circuito, nombre_circuito, codigo_parroquia) " +
          s"VALUES(${indice + 1},'${tupla._1}', '${tupla._2}', '${tupla._3}', '${tupla._4}', '${tupla._5}', '${tupla._6}', " +
          s"'${tupla._7}', '${tupla._8}', ${codigoParroquia});"
        escribri_distrito.write(formatIn + " " + "\n")
        csvWriter.writeRow(List(indice + 1, tupla._1, tupla._2, tupla._3, tupla._4, tupla._5, tupla._6, tupla._7, tupla._8, codigoParroquia))
      }
    } finally {
      escribri_distrito.close()
      csvWriter.close()
    }
  }
  //distrito(contentFile)

  // -------------------------------------***************************** TIPO_ARMA ***********************************---------------------------
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
  //tipo_arma(contentFile)

  // -------------------------------------***************************** ARMA ***********************************---------------------------
  def Arma(data: List[Map[String, String]]): Unit = {
    val csvFile = new File("src/main/resorces2/Arma.csv") // Replace with the correct path
    val csvWriter = CSVWriter.open(csvFile)

    // Define mappings for arma types
    val armaTypes = Map(
      "NINGUNA" -> 1,
      "ARMAS DE FUEGO" -> 2,
      "ARMAS BLANCAS (OBJETO CORTANTE O PUNZANTE)" -> 3
    )

    // Process data and handle missing or incomplete rows
    val intermediario: List[(String, String)] = data.flatMap { hasMap =>
      for {
        arma <- hasMap.get("arma")
        tipoArma <- hasMap.get("tipo_arma").orElse(Some("NINGUNA")) // Default to "NINGUNA" if tipo_arma is missing
      } yield (arma, tipoArma)
    }.distinct.sorted

    // Write header row to CSV
    csvWriter.writeRow(List("arma_id", "nombre", "tipo_arma_id"))

    // Generate SQL insert statements and write to CSV
    intermediario.zipWithIndex.foreach { case ((arma, tipoArma), indice) =>
      val tipoArmaId = armaTypes.getOrElse(tipoArma.toUpperCase, 1) // Default to "NINGUNA" id (1) if not found
      val formatIn = s"INSERT INTO armas(nombre, arma_id, tipo_arma_id) VALUES('${arma}', ${indice + 1}, ${tipoArmaId});"

      // Write to SQL file
      escribri_arma.write(formatIn + "\n")

      // Write to CSV file
      csvWriter.writeRow(List(indice + 1, arma, tipoArma))
    }

    // Close CSV and SQL writers
    csvWriter.close()
    escribri_arma.close()
  }

  //Arma(contentFile)
  // -------------------------------------***************************** NACIONALIDAD ***********************************---------------------------
  def nacionalidad(data: List[Map[String, String]]): Unit = {
    val nacionalidades: List[(String, Int)] = data.map { hasMap =>
      hasMap("nacionalidad")
    }.distinct.sorted.zipWithIndex

    // ----------------------- CAV SECUNDARIO DE NACIONALIDADES ----------------------------------------
    val csvFile = new File("src/main/resorces2/Nacionalidad.csv") // Reemplaza con la ruta correcta
    val csvWriter = CSVWriter.open(csvFile)
    try {
      csvWriter.writeRow(List("nacionalidad_id", "nombre"))
      nacionalidades.foreach { case (nombre, indice) =>
        csvWriter.writeRow(List(indice + 1, nombre))
      }
    } finally {
      csvWriter.close()
    }

    try {
      nacionalidades.foreach { case (nombre, indice) =>
        val formatIn: String = s"INSERT INTO nacionalidad(nacionalidad_id, nombre) VALUES(${indice + 1}, '${nombre}');"
        escribri_2.write(formatIn + " " + "\n")
      }
    } finally {
      escribri_2.close()
    }
  }

  //nacionalidad(contentFile)
  // -------------------------------------***************************** DELITO ***********************************---------------------------
  def cargarDistrito(filePath: String): List[(Int, String, String, String, String, String, String, String, String, String)] = {
    val readerDist: CSVReader = CSVReader.open(new File(filePath))(new DefaultCSVFormat {
      override val delimiter: Char = ','
    })
    val data: List[Map[String, String]] = readerDist.allWithHeaders()

    val distrito: List[(Int, String, String, String, String, String, String, String, String, String)] = data.flatMap { hasMap =>
      val codigoParroquiaOpt = hasMap.getOrElse("codigo_parroquia", "")

      if (hasMap.contains("distrito_id") && hasMap.contains("codigo_distrito") && hasMap.contains("nombre_distrito") &&
        hasMap.contains("nombre_zona") && hasMap.contains("nombre_subzona") && hasMap.contains("codigo_subcircuito") &&
        hasMap.contains("nombre_subcircuito") && hasMap.contains("codigo_circuito") && hasMap.contains("nombre_circuito")) {
        Some((
          hasMap("distrito_id").toInt,
          hasMap("codigo_distrito"),
          hasMap("nombre_distrito"),
          hasMap("nombre_zona"),
          hasMap("nombre_subzona"),
          hasMap("codigo_subcircuito"),
          hasMap("nombre_subcircuito"),
          hasMap("codigo_circuito"),
          hasMap("nombre_circuito"),
          codigoParroquiaOpt
        ))
      } else {
        println(s"Error: Fila incompleta en el archivo de distritos: $hasMap")
        None
      }
    }
    readerDist.close()
    distrito
  }

  def Delito(data: List[Map[String, String]]): Unit = {
    val distrito = cargarDistrito("src/main/resorces2/Distrito.csv")

    val delitoAndDistrito: List[(String, String, String, String, String, String, String, String, Option[Int])] = data.flatMap { hasMap =>
      val codigoParroquiaOpt = hasMap.get("codigo_parroquia").filter(_.nonEmpty).map(_.toInt)

      distrito.find { case (_, codigoDistrito, nombreDistrito, nombreZona, nombreSubzona, codigoSubcircuito, nombreSubcircuito, codigoCircuito, nombreCircuito, _) =>
        codigoDistrito == hasMap.getOrElse("codigo_distrito", "") &&
          nombreDistrito == hasMap.getOrElse("nombre_distrito", "") &&
          nombreZona == hasMap.getOrElse("nombre_zona", "") &&
          nombreSubzona == hasMap.getOrElse("nombre_subzona", "") &&
          codigoSubcircuito == hasMap.getOrElse("codigo_subcircuito", "") &&
          nombreSubcircuito == hasMap.getOrElse("nombre_subcircuito", "") &&
          codigoCircuito == hasMap.getOrElse("codigo_circuito", "") &&
          (codigoParroquiaOpt.isEmpty || codigoParroquiaOpt.contains(codigoParroquiaOpt.getOrElse(-1)))
      } match {
        case Some((distrito_id, _, _, _, _, _, _, _, _, _)) =>
          Some((
            hasMap("presunta_flagrancia"),
            hasMap("presunta_infraccion"),
            hasMap("presunta_subinfraccion"),
            hasMap("presunta_modalidad"),
            hasMap("codigo_iccs"),
            hasMap("movilizacion"),
            hasMap("tipo"),
            hasMap("condicion"),
            Some(distrito_id)
          ))
        case None =>
          println(s"No se encontró distrito para (${hasMap.getOrElse("codigo_distrito", "")}, ${hasMap.getOrElse("nombre_distrito", "")}, ${hasMap.getOrElse("nombre_zona", "")}, ${hasMap.getOrElse("nombre_subzona", "")}, ${hasMap.getOrElse("codigo_subcircuito", "")}, ${hasMap.getOrElse("nombre_subcircuito", "")}, ${hasMap.getOrElse("codigo_circuito", "")}, ${hasMap.getOrElse("nombre_circuito", "")}, ${codigoParroquiaOpt.getOrElse("")})")
          None
      }
    }.distinct

    // ---------------------- CSV SECUNDARIO DE DELITO ----------------------
    val csvFile = new File("C:\\cuarto_semestre\\proyectoIntegrador\\datosGenerados\\Delito.csv") // Reemplaza con la ruta correcta
    val csvWriter = CSVWriter.open(csvFile)
    try {
      csvWriter.writeRow(List("delito_id", "presunta_flagrancia", "presunta_infraccion", "presunta_subinfraccion", "presunta_modalidad", "codigo_iccs", "movilizacion", "tipo", "condicion", "distrito_id"))
      delitoAndDistrito.zipWithIndex.foreach { case ((presunta_flagrancia, presunta_infraccion, presunta_subinfraccion, presunta_modalidad, codigo_iccs, movilizacion, tipo, condicion, distrito_idOpt), index) =>
        val distrito_id = distrito_idOpt.getOrElse(-1)
        val distrito_id_2 = distrito_idOpt.map(_.toString).getOrElse("''")
        csvWriter.writeRow(List(index + 1, presunta_flagrancia, presunta_infraccion, presunta_subinfraccion, presunta_modalidad, codigo_iccs, movilizacion, tipo, condicion, distrito_id))
        val formatIn: String = s"INSERT INTO delito(delito_id, presunta_flagrancia, presunta_infraccion, presunta_subinfraccion, presunta_modalidad, codigo_iccs, movilizacion, tipo, condicion, distrito_id) " +
          s"VALUES(${index + 1}, '$presunta_flagrancia', '$presunta_infraccion', '$presunta_subinfraccion', '$presunta_modalidad', '$codigo_iccs', '$movilizacion', '$tipo', '$condicion', $distrito_id_2);"
        escribri_Delito.write(formatIn + "\n")
      }
    } finally {
      csvWriter.close()
      escribri_Delito.close()
    }
  }
  //Delito(contentFile)

  // -------------------------------------***************************** PERSONA  ***********************************---------------------------

  def cargarNacionalidades(filePath: String): List[(Int, String)] = {
    val readerCompl: CSVReader = CSVReader.open(new File(filePath))
    val data: List[Map[String, String]] = readerCompl.allWithHeaders()

    val nacionalidad: List[(Int, String)] = data.flatMap { hasMap =>
      for {
        idStr <- hasMap.get("nacionalidad_id")
        id = idStr.toInt
        nombre <- hasMap.get("nombre")
      } yield (id, nombre)
    }
    readerCompl.close()
    nacionalidad
  }

  def Person(data: List[Map[String, String]]): Unit = {
    val nacionalidades = cargarNacionalidades("src/main/resorces2/Nacionalidad.csv")

    val persona: List[(String, String, Option[String], String, String, String, String, String)] = data.map { hasMap =>
      (
        hasMap.getOrElse("estado_civil", ""),
        hasMap.getOrElse("estatus_migratorio", ""),
        hasMap.get("edad").filter(_.nonEmpty),
        hasMap.getOrElse("sexo", ""),
        hasMap.getOrElse("autoidentificacion_etnica", ""),
        hasMap.getOrElse("nivel_de_instruccion", ""),
        hasMap.getOrElse("nacionalidad", ""),
        hasMap.getOrElse("genero", "")
      )
    }.distinct

    // ------------------- Escritura en CSV y SQL ----------------------
    val csvFile = new File("C:\\cuarto_semestre\\proyectoIntegrador\\datosGenerados\\Personas.csv")
    val csvWriter = CSVWriter.open(csvFile, append = true)
    try {
      if (persona.nonEmpty) {
        csvWriter.writeRow(List("persona_id", "estado_civil", "estatus_migratorio", "edad", "sexo", "autoidentificacion_etnica", "nivel_de_instruccion", "nacionalidad_id", "genero"))
      }
      persona.zipWithIndex.foreach { case ((estadoCivil, estatusMigratorio, edadOpt, sexo, autoidentificacionEtnica, nivelDeInstruccion, nacionalidad, genero), indice) =>
        nacionalidades.find(_._2 == nacionalidad) match {
          case Some((idNacionalidad, _)) =>
            val edadSql = edadOpt.getOrElse("NULL")

            csvWriter.writeRow(List(indice + 1, estadoCivil, estatusMigratorio, edadOpt.getOrElse(""), sexo, autoidentificacionEtnica, nivelDeInstruccion, idNacionalidad.toString, genero))

            val formatIn: String = s"INSERT INTO person(persona_id, estado_civil, estatus_migratorio, edad, sexo, autoidentificacion_etnica, nivel_de_instruccion, nacionalidad_id, genero) " +
              s"VALUES(${indice + 1}, '$estadoCivil', '$estatusMigratorio', $edadSql, '$sexo', '$autoidentificacionEtnica', '$nivelDeInstruccion', $idNacionalidad, '$genero');"
            escribi_persona.write(formatIn + "\n")
          case None =>
            println(s"No se encontró nacionalidad para '$nacionalidad'.")
        }
      }
    } finally {
      csvWriter.close()
      escribi_persona.close()
    }
  }
  //nacionalidad(contentFile)
  // Person(contentFile)

  // -------------------------------------***************************** DETENCIONES  ***********************************---------------------------
  def cargarPerson(filePath: String): List[(Int, String, String, String, String, String, String, String, String)] = {
    val readerPer: CSVReader = CSVReader.open(new File(filePath))(new DefaultCSVFormat {
      override val delimiter: Char = ','
    })
    val data: List[Map[String, String]] = readerPer.allWithHeaders()
    readerPer.close()

    data.flatMap { hasMap =>
      try {
        Some((
          hasMap("persona_id").toInt,
          hasMap("estado_civil"),
          hasMap("estatus_migratorio"),
          hasMap("edad"),
          hasMap("sexo"),
          hasMap("autoidentificacion_etnica"),
          hasMap("nivel_de_instruccion"),
          hasMap("nacionalidad_id"),
          hasMap("genero")
        ))
      } catch {
        case _: NumberFormatException => None
      }
    }
  }

  def cargarArmas(filePath: String): List[(Int, String, String)] = {
    val readerPer: CSVReader = CSVReader.open(new File(filePath))(new DefaultCSVFormat {
      override val delimiter: Char = ','
    })
    val data: List[Map[String, String]] = readerPer.allWithHeaders()

    val armas: List[(Int, String, String)] = data.map { hasMap =>
      (
        hasMap("arma_id").toInt,
        hasMap("nombre"),
        hasMap("tipo_arma_id")
      )
    }
    readerPer.close()
    armas
  }

  def cargarDelitos(filePath: String): List[(Int, String, String, String, String, String, String, String, String, Int)] = {
    val readerDel: CSVReader = CSVReader.open(new File(filePath))(new DefaultCSVFormat {
      override val delimiter: Char = ','
    })
    val data: List[Map[String, String]] = readerDel.allWithHeaders()

    val delitos: List[(Int, String, String, String, String, String, String, String, String, Int)] = data.map { hasMap =>
      (
        hasMap("delito_id").toInt,
        hasMap("presunta_flagrancia"),
        hasMap("presunta_infraccion"),
        hasMap("presunta_subinfraccion"),
        hasMap("presunta_modalidad"),
        hasMap("codigo_iccs"),
        hasMap("movilizacion"),
        hasMap("tipo"),
        hasMap("condicion"),
        hasMap("distrito_id").toInt
      )
    }
    readerDel.close()
    delitos
  }

  def Detencion(data: List[Map[String, String]]): Unit = {
    val personas = cargarPerson("C:\\cuarto_semestre\\proyectoIntegrador\\datosGenerados\\Personas.csv")
    val armas = cargarArmas("src/main/resorces2/Arma.csv")
    val delitos = cargarDelitos("C:\\cuarto_semestre\\proyectoIntegrador\\datosGenerados\\Delito.csv")

    // Helper function to find IDs or return None
    def findPersonId(hasMap: Map[String, String]): Option[Int] = {
      personas.find { case (_, estado_civil, estatus_migratorio, edad, sexo, autoidentificacion_etnica, nivel_de_instruccion, nacionalidad_id, genero) =>
        estado_civil == hasMap.getOrElse("estado_civil", "") &&
          estatus_migratorio == hasMap.getOrElse("estatus_migratorio", "") &&
          edad == hasMap.getOrElse("edad", "") &&
          sexo == hasMap.getOrElse("sexo", "") &&
          autoidentificacion_etnica == hasMap.getOrElse("autoidentificacion_etnica", "") &&
          nivel_de_instruccion == hasMap.getOrElse("nivel_de_instruccion", "") &&
          nacionalidad_id == hasMap.getOrElse("nacionalidad_id", "") &&
          genero == hasMap.getOrElse("genero", "")
      }.map(_._1)
    }

    def findArmaId(hasMap: Map[String, String]): Option[Int] = {
      armas.find { case (_, nombre, tipo_arma_id) =>
        nombre == hasMap.getOrElse("arma", "") &&
          tipo_arma_id == hasMap.getOrElse("tipo_arma", "")
      }.map(_._1)
    }

    def findDelitoId(hasMap: Map[String, String]): Option[Int] = {
      delitos.find { case (_, presunta_flagrancia, presunta_infraccion, presunta_subinfraccion, presunta_modalidad, codigo_iccs, movilizacion, tipo, condicion, _) =>
        presunta_flagrancia == hasMap.getOrElse("presunta_flagrancia", "") &&
          presunta_infraccion == hasMap.getOrElse("presunta_infraccion", "") &&
          presunta_subinfraccion == hasMap.getOrElse("presunta_subinfraccion", "") &&
          presunta_modalidad == hasMap.getOrElse("presunta_modalidad", "") &&
          codigo_iccs == hasMap.getOrElse("codigo_iccs", "") &&
          movilizacion == hasMap.getOrElse("movilizacion", "") &&
          tipo == hasMap.getOrElse("tipo", "") &&
          condicion == hasMap.getOrElse("condicion", "")
      }.map(_._1)
    }

    val detenciones: List[(Int, String, String, String, String, String, String, Int, Int, Int)] = data.flatMap { hasMap =>
      val persona_id: Option[Int] = findPersonId(hasMap)
      val arma_id: Option[Int] = findArmaId(hasMap)
      val delito_id: Option[Int] = findDelitoId(hasMap)

      Some(
        hasMap.getOrElse("codigo_parroquia", ""),
        hasMap("nombre_canton"),
        hasMap("fecha_detencion_aprehension"),
        hasMap("hora_detencion_aprehension"),
        hasMap("lugar"),
        hasMap("tipo_lugar"),
        hasMap.getOrElse("numero_detenciones", ""),
        persona_id.getOrElse(-1),
        arma_id.getOrElse(-1),
        delito_id.getOrElse(-1)
      )
    }.zipWithIndex.map {
      case ((codigo_parroquia, nombre_canton, fecha_detencion_aprehension, hora_detencion_aprehension, lugar, tipo_lugar, numero_detenciones, persona_id, arma_id, delito_id), index) =>
        if (codigo_parroquia.isEmpty) {
          val pathCanton: String = "C:\\cuarto_semestre\\proyectoIntegrador\\scalaCargaDatos\\src\\main\\resorces2\\Canton.csv"
          val readerCanton: CSVReader = CSVReader.open(new File(pathCanton))(new DefaultCSVFormat {
            override val delimiter: Char = ','
          })
          val dataCantones: List[Map[String, String]] = readerCanton.allWithHeaders()
          val cantonMap = dataCantones.collectFirst {
            case row if row("nombre_canton") == nombre_canton => row("codigo_canton")
          }

          cantonMap match {
            case Some(codigo_canton) =>
              (index, codigo_canton, fecha_detencion_aprehension, hora_detencion_aprehension, lugar, tipo_lugar, numero_detenciones, persona_id, arma_id, delito_id)
            case None =>
              (index, "", fecha_detencion_aprehension, hora_detencion_aprehension, lugar, tipo_lugar, numero_detenciones, persona_id, arma_id, delito_id)
          }
        } else {
          (index, "", fecha_detencion_aprehension, hora_detencion_aprehension, lugar, tipo_lugar, numero_detenciones, persona_id, arma_id, delito_id)
        }
    }

    // Write to CSV and SQL
    val csvFile = new File("C:\\cuarto_semestre\\proyectoIntegrador\\datosGenerados\\Detencion.csv")
    val csvWriter = CSVWriter.open(csvFile)
    try {
      detenciones.foreach {
        case (indice, codigo_canton, fecha_detencion_aprehension, hora_detencion_aprehension, lugar, tipo_lugar, numero_detenciones, persona_id, arma_id, delito_id) =>

          // Transformaciones necesarias para las fechas
          val string_nuevo = fecha_detencion_aprehension.replaceAll("T", " ").replaceAll("000", " ").toCharArray.filter(_ != '.').mkString
          val string_nuevo_2 = hora_detencion_aprehension.replaceAll("T", " ").replaceAll("000", " ").toCharArray.filter(_ != '.').mkString

          // Lógica para manejar valores vacíos o -1
          val codigo_canton_sql = if (codigo_canton.isEmpty) "NULL" else codigo_canton
          val lugar_sql = if (lugar.isEmpty) "NULL" else s"'$lugar'"
          val tipo_lugar_sql = if (tipo_lugar.isEmpty) "NULL" else s"'$tipo_lugar'"
          val numero_detenciones_sql = if (numero_detenciones.toString.isEmpty) "NULL" else numero_detenciones.toString
          val persona_id_sql = if (persona_id.toString.isEmpty || persona_id == -1) "NULL" else persona_id.toString
          val arma_id_sql = if (arma_id.toString.isEmpty || arma_id == -1) "NULL" else arma_id.toString
          val delito_id_sql = if (delito_id.toString.isEmpty || delito_id == -1) "NULL" else delito_id.toString
          val fecha_detencion_sql = if (string_nuevo.isEmpty) "NULL" else s"TIMESTAMP '$string_nuevo'"
          val hora_detencion_sql = if (string_nuevo_2.isEmpty) "NULL" else s"TIMESTAMP '$string_nuevo_2'"

          // Construcción de la sentencia INSERT
          val formatIn = s"INSERT INTO detencion(detencion_id, codigo_canton, fecha_detencion_aprehension, hora_detencion_aprehension, lugar, tipo_lugar, numero_detenciones, persona_id, arma_id, delito_id) " +
            s"VALUES($indice, $codigo_canton_sql, $fecha_detencion_sql, $hora_detencion_sql, $lugar_sql, $tipo_lugar_sql, $numero_detenciones_sql, $persona_id_sql, $arma_id_sql, $delito_id_sql);"

          escribri_detencion.write(formatIn + "\n")
      }
    } finally {
      escribri_detencion.close()
    }
  }
    Detencion(contentFile)

    //complementarios(contentFileComplementarios)


    //////////////////////////////////////////////////////////

}
