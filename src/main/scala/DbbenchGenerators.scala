import java.io.{File, FileWriter}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.JavaConverters._

case class DbConfig(machine: String, host: String, user: String, pass: String)

object DbbenchGenerators extends App {

  val om = new ObjectMapper()
  om.registerModule(new DefaultScalaModule)

  start()

  def loadDbConfigs(): List[DbConfig] = {
    om.readTree(new File("config.json")).fields().asScala.map { field =>
      val value = field.getValue
      DbConfig(field.getKey, value.get("host").asText, value.get("user").asText, value.get("pass").asText
      )
    }.toList
  }

  def start(): Unit = {
    if (args.isEmpty)
      println(
        """
          |Generate products CSVs:
          |dbbench-generators gencsv [numstores numproducts] [numstores numproducts] ...
          |
          |Fill tables from CSV:
          |dbbench-generators loadcsv csvfile
          |
          |Generate scripts:
          |dbbench-generators scripts
          |
          |""".stripMargin
      )
    else {
      args.head match {
        case "gencsv" =>
          args.drop(1).grouped(2).foreach { x =>
            if (x.length == 2) {
              val numStores = x(0).toInt
              val numproducts = x(1).toInt
              generateManyStoresCsv(numStores, numproducts)
            }
          }

        case "loadcsv" =>

        case "scripts" =>
          val configs = loadDbConfigs()

          createCommonScript("create-tables.sh", configs.map { config => generateScriptForCreateTables(config) })

          createCommonScript("drop-tables.sh", configs.map { config => generateScriptForDropTables(config) })

          val insertScripts =
            configs.map { config => generateScriptForInsertTables(config, 1, "stores-20000.csv") } ++
            configs.map { config => generateScriptForInsertTables(config, 8, "stores-1000.csv") }
          createCommonScript("insert-tables.sh", insertScripts.flatten)
      }
    }
  }

  def generateManyStoresCsv(numStores: Int, numproducts: Int): Unit = {
    println(s"Generating $numStores stores with $numproducts products ...")
    val fw = new FileWriter(s"stores-$numproducts.csv")
    (1 to numStores).foreach { _ =>
      generateOneStoreCsv(UUID.randomUUID.toString, numproducts, fw)
    }
    fw.close()
  }

  def generateOneStoreCsv(storeId: String, count: Int, fw: FileWriter): Unit = {
    val now = System.currentTimeMillis()
    Stream.from(0).take(count).grouped(100).foreach{ x =>
      x.map(_ => ProductGenerator.gen).foreach { p =>
        val productId = p.productId
        val json = om.writeValueAsString(p)
        val jsonDoubleQuotes = json.replace("\"","\"\"")
        fw.write(storeId)
        fw.write(",")
        fw.write(productId)
        fw.write(",")
        fw.write("\"")
        fw.write(jsonDoubleQuotes)
        fw.write("\"")
        fw.write(",")
        fw.write(s""""{""created"":$now}"""")
        fw.write("\n")
      }
    }
  }

  def generateScriptForCreateTables(config: DbConfig): String = {
    val machine = config.machine
    val baseName = s"create-tables-$machine"
    val iniFile = s"ini/create-tables-$machine.ini"
    val scriptName = s"create-tables-$machine.sh"
    writeFile(iniFile,
      s"""[setup]
        |${createTable(machine, compressed = true, binarypk = true)}
        |${createTable(machine, compressed = true, binarypk = false)}
        |${createTable(machine, compressed = false, binarypk = true)}
        |${createTable(machine, compressed = false, binarypk = false)}
        |""".stripMargin)
    writeDbBenchScript(config, baseName)
    scriptName
  }

  def generateScriptForDropTables(config: DbConfig): String = {
    val machine = config.machine
    val baseName = s"drop-tables-$machine"
    val iniFile = s"ini/drop-tables-$machine.ini"
    val scriptName = s"drop-tables-$machine.sh"
    writeFile(iniFile,
      s"""[setup]
        |${dropTable(machine, compressed = true, binarypk = true)}
        |${dropTable(machine, compressed = true, binarypk = false)}
        |${dropTable(machine, compressed = false, binarypk = true)}
        |${dropTable(machine, compressed = false, binarypk = false)}
        |""".stripMargin)
    writeDbBenchScript(config, baseName)
    scriptName
  }

  def generateScriptForInsertTables(config: DbConfig, concurrency: Int, csv: String): Seq[String] = {
    val scripts =
      createInsertScript(config, compressed = true, binarypk = true, concurrency, csv) ::
      createInsertScript(config, compressed = true, binarypk = false, concurrency, csv) ::
      createInsertScript(config, compressed = false, binarypk = true, concurrency, csv) ::
      createInsertScript(config, compressed = false, binarypk = false, concurrency, csv) ::
      Nil
    scripts.flatten
  }

  def writeDbBenchScript(config: DbConfig, baseName: String): Unit = {
    writeFile(baseName+".sh",
      s"""#!/bin/sh
         |dbbench --database sdlpoc --host ${config.host} --port 3306 --username ${config.user} --password ${config.pass} --intermediate-stats=false $baseName.ini
         |""".stripMargin)
  }

  def createTableName(machine: String, compressed: Boolean, binarypk: Boolean): String = {
    val zip = if (compressed) "y" else "n"
    val bin = if (binarypk) "y" else "n"
    s"products_${machine}_zip${zip}_bin${bin}"
  }

  def createTable(machine: String, compressed: Boolean, binarypk: Boolean): String = {
    val percona: Boolean = machine.contains("percona")
    val idtype = if (binarypk) "varbinary(16)" else "varchar(36)"
    val tableName = createTableName(machine, compressed, binarypk)
    val columnCompession = if (percona && compressed) "COLUMN_FORMAT COMPRESSED" else ""
    val tableCompession = if (!percona && compressed) "ROW_FORMAT=COMPRESSED" else ""
    val engine = if (machine.contains("innodb")) "InnoDB" else "RocksDB"
    if (engine == "RocksDB" && compressed)
      ""
    else
      s"""query=CREATE TABLE IF NOT EXISTS $tableName ( \\
        |  tenant_id $idtype NOT NULL, \\
        |  entity_id $idtype NOT NULL, \\
        |  is_deleted boolean default false, \\
        |  version bigint not null default 1, \\
        |  entity json NOT NULL $columnCompession, \\
        |  metadata json NOT NULL $columnCompession, \\
        |  price DECIMAL(10,2) GENERATED ALWAYS AS ((CAST(entity->"$$.price" AS DECIMAL(10,2)))), \\
        |  created BIGINT GENERATED ALWAYS AS (metadata->"$$.created" ), \\
        |  primary key(tenant_id, entity_id), \\
        |  INDEX idx_name  (tenant_id, (CAST(entity->>"$$.name" AS CHAR(80)) collate utf8mb4_bin)), \\
        |  INDEX idx_price (tenant_id, price), \\
        |  INDEX idx_created (tenant_id, created), \\
        |  INDEX idx_collections (tenant_id , (CAST(entity->'$$.collections' AS CHAR(36) ARRAY))), \\
        |  INDEX idx_option_name (tenant_id , (CAST(entity->'$$.options[*].title' AS CHAR(80) ARRAY))), \\
        |  INDEX idx_option_sel_name (tenant_id ,(CAST(entity->'$$.options[*].selections[*].value' AS CHAR(80) ARRAY))) \\
        |) ENGINE=$engine DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin $tableCompession
        |""".stripMargin
  }

  def createInsertScript(config: DbConfig, compressed: Boolean, binarypk: Boolean, concurrency: Int, csv: String): Option[String] = {
    val machine = config.machine
    val engine = if (machine.contains("innodb")) "InnoDB" else "RocksDB"
    if (engine == "RocksDB" && compressed)
      None
    else {
      val tableName = createTableName(machine, compressed, binarypk)
      val baseName = s"insert-${tableName}-${concurrency}"
      val iniFile = "ini/" + baseName + ".ini"
      val scriptFile = baseName + ".sh"

      val values = if (binarypk)
        "(unhex(replace(?,'-','')),unhex(replace(?,'-','')),?,?)"
      else
        "(?,?,?,?)"

      writeFile(iniFile,
        s"""
           |duration=60s
           |[$baseName]
           |query=insert into $tableName (tenant_id,entity_id,entity,metadata) values $values
           |query-args-file=$csv
           |concurrency=$concurrency
           |count=1
           |query-results-file=results/$baseName.csv
           |""".stripMargin)

      writeDbBenchScript(config, baseName)
      Some(scriptFile)
    }
  }

  def dropTable(machine: String, compressed: Boolean, binarypk: Boolean): String = {
    val tableName = createTableName(machine, compressed, binarypk)
    val engine = if (machine.contains("innodb")) "InnoDB" else "RocksDB"
    if (engine == "RocksDB" && compressed)
      ""
    else
      s"""query=DROP TABLE IF EXISTS $tableName
         |""".stripMargin
  }


  def createCommonScript(file: String, scripts: List[String]): Unit = {
    val content = scripts.map(s => s"./$s").mkString("#!/bin/bash\n", "\n", "\n")
    writeFile(file, content)
  }

  def writeFile(name: String, content: String): Unit = {
    val fw = new FileWriter(name)
    fw.write(content)
    fw.close()
  }
}