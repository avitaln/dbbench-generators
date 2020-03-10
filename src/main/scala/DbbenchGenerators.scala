import java.io.{File, FileWriter}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.JavaConverters._

case class Config(machine: String, host: String, user: String, pass: String)


object DbbenchGenerators extends App {

  val om = new ObjectMapper()
  om.registerModule(new DefaultScalaModule)

  val configs = om.readTree(new File("config.json")).fields().asScala.map { f =>
    val v = f.getValue
    Config(
      f.getKey,
      v.get("host").asText,
      v.get("user").asText,
      v.get("pass").asText
    )
  }.toList

  if (args.isEmpty)
    println(
      """
        |dbbench-generators products [numstores numproducts] [numstores numproducts] ...
        |dbbench-generators scripts numstores numstores ...
        |""".stripMargin
    )
  else {
    args.head match {
      case "products" =>
        args.drop(1).grouped(2).foreach { x =>
          if (x.length == 2) {
            val numStores = x(0).toInt
            val numproducts = x(1).toInt
            generateStores(numStores, numproducts)
          }
        }
      case "scripts" =>
        createCommonScript("create-tables.sh", configs.map { config => generateScriptForCreateTables(config) })
        createCommonScript("drop-tables.sh", configs.map { config => generateScriptForDropTables(config) })
        createCommonScript("insert-tables-1.sh", configs.map { config => generateScriptForInsertTables(config, 1, "stores-20000.csv") })
        createCommonScript("insert-tables-8.sh", configs.map { config => generateScriptForInsertTables(config, 8, "stores-1000.csv" ) })
        createCommonScript("insert-tables-16.sh", configs.map { config => generateScriptForInsertTables(config, 16, "stores-100.csv" ) })
    }
  }

  def generateStores(numStores: Int, numproducts: Int): Unit = {
    println(s"Generating $numStores stores with $numproducts products ...")
    val fw = new FileWriter(s"stores-$numproducts.csv")
    (1 to numStores).foreach { _ =>
      generateStore(UUID.randomUUID.toString, numproducts, fw)
    }
    fw.close()
  }

  def generateStore(storeId: String, count: Int, fw: FileWriter): Unit = {
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

  def generateScriptForCreateTables(config: Config): String = {
    val machine = config.machine
    val baseName = s"create-tables-$machine"
    val iniFile = s"create-tables-$machine.ini"
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

  def generateScriptForDropTables(config: Config): String = {
    val machine = config.machine
    val baseName = s"drop-tables-$machine"
    val iniFile = s"drop-tables-$machine.ini"
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

  def generateScriptForInsertTables(config: Config, concurrency: Int, csv: String): String = {
    val machine = config.machine
    val baseName = s"drop-tables-$machine"
    val iniFile = s"drop-tables-$machine.ini"
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

  def writeDbBenchScript(config: Config, baseName: String): Unit = {
    writeFile(baseName+".sh",
      s"""#!/bin/sh
         |dbbench --database sdlpoc --host ${config.host} --port 3306 --username ${config.user} --password ${config.pass} --intermediate-stats=false $baseName.ini
         |""".stripMargin)
  }

  def createTable(machine: String, compressed: Boolean, binarypk: Boolean): String = {
    val percona: Boolean = machine.contains("percona")
    val zip = boolToLetter(compressed)
    val bin = boolToLetter(binarypk)
    val idtype = if (binarypk) "varbinary(16)" else "varchar(36)"
    val tableName = s"products_zip${zip}_bin${bin}_${machine}"
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
        |  unique key(tenant_id, entity_id) \\
        |) ENGINE=$engine DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin $tableCompession
        |""".stripMargin
  }

  def dropTable(machine: String, compressed: Boolean, binarypk: Boolean): String = {
    val zip = boolToLetter(compressed)
    val bin = boolToLetter(binarypk)
    val tableName = s"products_zip${zip}_bin${bin}_${machine}"
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

  def boolToLetter(b: Boolean): String = if (b) "y" else "n"

  def generateScriptForInserts(machine: String, config: Config): Unit = {

  }

  def writeFile(name: String, content: String): Unit = {
    val fw = new FileWriter(name)
    fw.write(content)
    fw.close()
  }

}