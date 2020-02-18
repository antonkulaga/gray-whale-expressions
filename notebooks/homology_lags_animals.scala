//just a scala code extracted from spark-notebook file

object Cells {
  import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

  import org.apache.spark.sql.types.StructType

  import org.bdgenomics.adam.rdd.ADAMContext._

  import org.bdgenomics.adam.rdd.ADAMContextExtensions._

  import scala.reflect.runtime.universe._

  import comp.bio.aging.playground.extras.uniprot._

  import scala.collection.JavaConversions._

  import org.apache.spark.storage.StorageLevel

  /* ... new cell ... */

  def sparkHadoopConf(sc: SparkContext, acountName: String, accountKey: String) = {

    sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")

    sc.hadoopConfiguration.set("fs.azure.account.key." + acountName + ".blob.core.windows.net", accountKey)

    sc

  }

  /* ... new cell ... */

  def azurize(container: String, accountName: String, blobFile: String): String = "wasbs://"+container+"@"+accountName+".blob.core.windows.net/"+blobFile 

  

  def writeText2Azure[T]( rdd: RDD[T], container: String, accountName: String, blobFile: String ): String =

  {

    val url = azurize(container, accountName, blobFile)

    rdd.saveAsTextFile(url)

    url

  }

  

  def writeTsv2Azure( df: DataFrame, container: String, accountName: String, blobFile: String ): String =

  {

    val url = azurize(container, accountName, blobFile)

    df.write.option("sep","\t").option("header","true").csv(url)

    url

  }


  /* ... new cell ... */

  //val connString = "put your connection string here"

  //val key = "put your connection key here"


  /* ... new cell ... */

  val account = "pipelines1"

  def az(path: String): String = azurize("storage", account, path)


  /* ... new cell ... */

  sparkHadoopConf(sparkContext, account, key)

    

  val spark = SparkSession

    .builder()

    .appName("lags")

    .getOrCreate()

  /* ... new cell ... */

  def loadTranscripts(species: String) = spark.readTSV(az(s"expressions/transcripts/${species}_transcripts_all.tab"), true)

  /* ... new cell ... */

  val quant_base = az("quant")

  

  val bowhead_liver_path = quant_base + "/bowhead/liver/transcripts_quant/quant.sf"

  val gray_whale_liver_path = quant_base +"/gray_whale/liver/transcripts_quant/quant.sf"

  val naked_mole_rat_liver_path = quant_base +"/NMR/liver/transcripts_quant/quant.sf"

  val minke_whale_liver_path = quant_base +"/minke_whale/liver/transcripts_quant/quant.sf"

  val naked_mole_rat_ensembl_liver_path = quant_base + "/NMR_ensembl_female/liver/transcripts_quant/quant.sf"

  val cow_liver_path = quant_base + "/cow/liver/GSM1020724/quant.sf"

  val human_liver_path = quant_base + "/human/liver/GSM1698568/quant.sf"

  val mouse_liver_path = quant_base +"/mouse/liver/GSM1400574/quant.sf"


  /* ... new cell ... */

  val bowhead_kidney_path = quant_base + "/bowhead/kidney/transcripts_quant/quant.sf"

  val gray_whale_kidney_path = quant_base +"/gray_whale/kidney/transcripts_quant/quant.sf"

  val naked_mole_rat_kidney_path = quant_base +"/NMR/kidney/transcripts_quant/quant.sf"

  val minke_whale_kidney_path = quant_base +"/minke_whale/kidney/transcripts_quant/quant.sf"

  val naked_mole_rat_ensembl_kidney_path = quant_base + "/NMR_ensembl_female/kidney/transcripts_quant/quant.sf"

  val cow_kidney_path = quant_base + "/cow/kidney/GSM1020723/quant.sf"

  val human_kidney_path = quant_base + "/human/kidney/GSM1698570/quant.sf"

  val mouse_kidney_path= quant_base + "/mouse/kidney/GSM2195188/quant.sf" 


  /* ... new cell ... */

  import org.apache.spark.sql.functions._

  import spark.implicits._

  

  val toDouble = udf[Double, String]( _.toDouble)

  /* ... new cell ... */

  val bowhead_liver = spark.readTSV(bowhead_liver_path, true).withColumn("liver", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val gray_whale_liver = spark.readTSV(gray_whale_liver_path, true).withColumn("liver", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val naked_mole_rat_liver = spark.readTSV(naked_mole_rat_liver_path, true).withColumn("liver", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val minke_whale_liver = spark.readTSV( minke_whale_liver_path, true).withColumn("liver", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  

  val naked_mole_rat_ensembl_liver = spark.readTSV(naked_mole_rat_ensembl_liver_path, true).withColumn("liver", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val cow_liver = spark.readTSV( cow_liver_path, true).withColumn("liver", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val mouse_liver = spark.readTSV( mouse_liver_path, true).withColumn("liver", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val human_liver = spark.readTSV( human_liver_path, true).withColumn("liver", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")


  /* ... new cell ... */

  val naked_mole_rat_kidney = spark.readTSV(naked_mole_rat_kidney_path, true).withColumn("kidney", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")  

  val bowhead_kidney = spark.readTSV(bowhead_kidney_path, true).withColumn("kidney", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val gray_whale_kidney = spark.readTSV(gray_whale_kidney_path, true).withColumn("kidney", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val minke_whale_kidney = spark.readTSV(minke_whale_kidney_path, true).withColumn("kidney", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  

  val naked_mole_rat_ensembl_kidney = spark.readTSV(naked_mole_rat_ensembl_kidney_path, true).withColumn("kidney", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val cow_kidney = spark.readTSV( cow_kidney_path, true).withColumn("kidney", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val mouse_kidney = spark.readTSV( mouse_kidney_path, true).withColumn("kidney", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")

  val human_kidney = spark.readTSV( human_kidney_path, true).withColumn("kidney", toDouble($"TPM")).drop("TPM").withColumnRenamed("Name","transcript")


  /* ... new cell ... */

  def join_liver_kidney(liver: DataFrame, kidney: DataFrame): DataFrame =

      liver.join(kidney.withColumnRenamed("transcript", "kidney_transcript"), $"transcript" === $"kidney_transcript").select("transcript", "liver", "kidney")

  

  val gray_whale_expressions = join_liver_kidney(gray_whale_liver, gray_whale_kidney)

  val naked_mole_rat_expressions = join_liver_kidney(naked_mole_rat_liver, naked_mole_rat_kidney)

  val minke_whale_expressions = join_liver_kidney( minke_whale_liver,  minke_whale_kidney)

  val naked_mole_rat_ensembl_expressions = join_liver_kidney(naked_mole_rat_ensembl_liver, naked_mole_rat_ensembl_kidney)

  val mouse_expressions = join_liver_kidney(mouse_liver, mouse_kidney)

  val human_expressions = join_liver_kidney(human_liver, human_kidney)

  val cow_expressions = join_liver_kidney(cow_liver, cow_kidney)

  /* ... new cell ... */

  /*

  gray_whale_expressions.coalesce(1).write.option("sep","\t").option("header","true").csv(az("expressions/mixed/gray_whale_liver_kidney.tsv"))

  naked_mole_rat_expressions.coalesce(1).write.option("sep","\t").option("header","true").csv(az("expressions/mixed/naked_mole_rat_liver_kidney.tsv"))

  minke_whale_expressions.coalesce(1).write.option("sep","\t").option("header","true").csv(az("expressions/mixed/minke_whale_liver_kidney.tsv"))

  */

  /* ... new cell ... */

   val exp_columns = List(

      "genage",

      "transcript",

      "liver",

      "kidney",

      "identity",

      "aligment_length",

      "mismatches",

      "gaps",

      "start_query",

      "end_query",

      "start_target",

      "end_target"

    )

  /* ... new cell ... */

  val threshold = "45"

  val base = az(s"lags/animals/${threshold}/blast6")

  

  val gray_whale_path = base + "/" + "animals_in_gray_whale.blast6"

  val bowhead_alaska_liver_path = base + "/" + "animals_in_bowhead_alaska_liver.blast6"

  val bowhead_greenland_kidney_path = base + "/" + "animals_in_bowhead_greenland_kidney.blast6"

  val minke_whale_path = base + "/" + "animals_in_minky_whale.blast6"

  val naked_mole_rat_path = base + "/" +"animals_in_naked_mole_rat.blast6"

  val naked_mole_rat_ensembl_path = base + "/" + "animals_in_naked_mole_rat_ensembl.blast6"

  val mouse_path = base + "/" + "animals_in_mouse.blast6"

  val human_path = base + "/" + "animals_in_human.blast6"

  val cow_path = base + "/" + "animals_in_cow.blast6"

  /* ... new cell ... */

  val columns = List("genage", "transcript", "identity", "aligment_length", "mismatches", "gaps", "start_query", "end_query", "start_target", "end_target") //last two non valid

  

  def withHeaders(df: DataFrame, cols: List[String], num:Int = 0): DataFrame = cols match {

    case Nil => df

    case head::tail =>   withHeaders(df.withColumnRenamed("_c"+num, head), tail, num + 1)

  }

  /* ... new cell ... */

  val headers = false

  val gray_whale_lags = withHeaders(spark.readTSV(gray_whale_path, headers), columns)

  val bowhead_alaska_lags = withHeaders(spark.readTSV(bowhead_alaska_liver_path, headers), columns)

  val bowhead_greenland_lags = withHeaders(spark.readTSV(bowhead_greenland_kidney_path, headers), columns)

  val minky_whale_lags = withHeaders(spark.readTSV(minke_whale_path, headers), columns)

  val naked_mole_rat_lags = withHeaders(spark.readTSV(naked_mole_rat_path, headers), columns)

  val naked_mole_rat_ensembl_lags = withHeaders(spark.readTSV(naked_mole_rat_ensembl_path, headers), columns) 

  val mouse_lags = withHeaders(spark.readTSV(mouse_path, headers), columns)

  val human_lags = withHeaders(spark.readTSV(human_path, headers), columns)

  val cow_lags = withHeaders(spark.readTSV(cow_path, headers), columns)

  (gray_whale_lags.count, bowhead_alaska_lags.count, bowhead_greenland_lags.count, minky_whale_lags.count, naked_mole_rat_lags.count,

  naked_mole_rat_ensembl_lags.count, mouse_lags.count, human_lags.count, cow_lags.count

  )

  /* ... new cell ... */

  val gray_whale_lags_exp = gray_whale_lags.join(gray_whale_expressions, "transcript").select(exp_columns.head, exp_columns.tail:_*)

  val naked_mole_rat_lags_exp = naked_mole_rat_lags.join(naked_mole_rat_expressions, "transcript").select(exp_columns.head, exp_columns.tail:_*)

  val minke_whale_lags_exp = minky_whale_lags.join(minke_whale_expressions, "transcript").select(exp_columns.head, exp_columns.tail:_*)

  val naked_mole_rat_ensembl_lags_exp = naked_mole_rat_ensembl_lags.join(naked_mole_rat_ensembl_expressions, "transcript")

    .select(exp_columns.head, exp_columns.tail:_*)

  val human_lags_exp = human_lags.join(human_expressions, "transcript").select(exp_columns.head, exp_columns.tail:_*)

  val mouse_lags_exp = mouse_lags.join(mouse_expressions, "transcript").select(exp_columns.head, exp_columns.tail:_*)

  val cow_lags_exp = cow_lags.join(cow_expressions, "transcript").select(exp_columns.head, exp_columns.tail:_*)

  

  (gray_whale_lags_exp.count, naked_mole_rat_lags_exp.count, minke_whale_lags_exp.count, 

   naked_mole_rat_ensembl_lags_exp.count, human_lags_exp.count, mouse_lags_exp.count, cow_lags_exp.count)

  /* ... new cell ... */

  val rest = exp_columns.tail.tail.tail.tail

  val bowhead_liver_lags_exp = bowhead_liver.join(bowhead_alaska_lags,"transcript").select(exp_columns.head, "transcript"::"liver"::rest:_*)

  val bowhead_kidney_lags_exp = bowhead_kidney.join(bowhead_greenland_lags,"transcript").select(exp_columns.head, "transcript"::"kidney"::rest:_*)

  (bowhead_liver_lags_exp.count, bowhead_kidney_lags_exp.count)

  /* ... new cell ... */

   import spark.implicits._

  import org.apache.spark.sql.functions._

  import org.apache.spark.sql.ColumnName

  

  val genAgeExtract = udf[Double, String]((x: String) => x.replace(">", "").toDouble)

  

  val toLong = udf[Long, String]( _.toLong)

  

  def processAnimal(df: DataFrame) = {

    df.na.fill("")

      .withColumn("liver_TPM", toDouble($"liver")).drop("liver")

      .withColumn("kidney_TPM", toDouble($"kidney")).drop("kidney")

      .withColumn("ident", toDouble($"identity")).drop("identity")

      .withColumn("aligment_len", toLong($"aligment_length")).drop("aligment_length")

      .withColumn("start_q", toLong($"start_query")).drop("start_query")

      .withColumn("end_q", toLong($"end_query")).drop("end_query")

      .withColumn("start_t", toLong($"start_target")).drop("start_target")

      .withColumn("end_t", toLong($"end_target")).drop("end_target")

      .withColumn("_tmp", split($"genage", "\\|"))

      .select(

        genAgeExtract($"_tmp".getItem(0)).as("GenAge ID"),

        $"transcript",

        $"liver_TPM",

        $"kidney_TPM",

        ($"liver_TPM" + $"kidney_TPM").as("avg_expression"),

        $"ident",

        $"aligment_len",

        $"mismatches",

        $"gaps",

        $"start_q",

        $"end_q",

        $"start_t",

        $"end_t"

    ).drop("_tmp").drop($"genage")

    .sort($"avg_expression".desc)

  }

  /* ... new cell ... */

  val results_columns = List(

    "GenAge ID",

    "transcript",

    "liver_TPM",

    "kidney_TPM",

    "avg_expression",

    "ident",

    "aligment_len",

    "mismatches",

    "gaps",

    "start_q",

    "end_q",

    "start_t",

    "end_t"

  )

  val lags_models_columns = List("GenAge ID", "symbol", "name", "organism", "entrez gene id", "avg lifespan change (max obsv)", "lifespan effect", "longevity influence")

  val lags_columns: List[String] = lags_models_columns ++ results_columns.tail


  /* ... new cell ... */

  val lags_models_columns = List("GenAge ID", "symbol", "name", "organism", "entrez gene id", "avg lifespan change (max obsv)", "lifespan effect", "longevity influence")

  val lags_columns: List[String] = lags_models_columns ++ results_columns.tail

  

  val lags_models = spark.readTSV(az("lags/genage_models.tsv"), true)

  def join_with_models(df: DataFrame) = {

    lags_models.join(df, "GenAge ID").select(lags_columns.head, lags_columns.tail:_*)

  }

  lags_models

  /* ... new cell ... */

  def process_with_models(df: DataFrame) = join_with_models( processAnimal(df))

  //"1978|ENSMUST00000173997.1|Rbm38|ENSMUSG00000027510.17|56190|Mus_musculus|22.0|Decrease|Pro-Longevity|chr2|173022437|173033709|FORWARD"

  /* ... new cell ... */

  val fields =  spark.readTSV(az(s"lags/genage_models_export_full.tsv"), true)

  ///storage/lags

  /* ... new cell ... */

  val extra = fields.select("Gene ID","Unigene ID","Ensembl ID","Alias", "Phenotype Description", "Max Lifespan Change", "Method", "Bibliographic reference")

  


  /* ... new cell ... */

  val gray_whale_animal_lags = process_with_models(gray_whale_lags_exp).join(extra, $"Gene ID"===$"GenAge ID").drop($"Gene ID")

  val minky_whale_animal_lags = process_with_models(minke_whale_lags_exp).join(extra, $"Gene ID"===$"GenAge ID").drop($"Gene ID")

  val naked_mole_rat_animal_lags = process_with_models(naked_mole_rat_lags_exp).join(extra, $"Gene ID"===$"GenAge ID").drop($"Gene ID")

  val naked_mole_rat_ensembl_animal_lags = process_with_models(naked_mole_rat_ensembl_lags_exp).join(extra, $"Gene ID"===$"GenAge ID").drop($"Gene ID")

  val human_animal_lags = process_with_models(human_lags_exp).join(extra, $"Gene ID"===$"GenAge ID").drop($"Gene ID")

  val mouse_animal_lags = process_with_models(mouse_lags_exp).join(extra, $"Gene ID"===$"GenAge ID").drop($"Gene ID")

  val cow_animal_lags = process_with_models(cow_lags_exp).join(extra, $"Gene ID"===$"GenAge ID").drop($"Gene ID")


  /* ... new cell ... */

  gray_whale_animal_lags.coalesce(1).write.option("sep","\t").option("header","true").csv(az(s"lags/animals/${threshold}/lags/gray_whale_animal_lags.tsv"))

  minky_whale_animal_lags.coalesce(1).write.option("sep","\t").option("header","true").csv(az(s"lags/animals/${threshold}/lags/minky_whale_animal_lags.tsv"))

  naked_mole_rat_animal_lags.coalesce(1).write.option("sep","\t").option("header","true").csv(az(s"lags/animals/${threshold}/lags/naked_mole_rat_animal_lags.tsv"))


  /* ... new cell ... */

  naked_mole_rat_ensembl_animal_lags.coalesce(1).write.option("sep","\t").option("header","true")

  .csv(az(s"lags/animals/${threshold}/lags/naked_mole_rat_ensembl_animal_lags.tsv"))

  human_animal_lags.coalesce(1).write.option("sep","\t").option("header","true")

  .csv(az(s"lags/animals/${threshold}/lags/human_animal_lags.tsv"))

  mouse_animal_lags.coalesce(1).write.option("sep","\t").option("header","true")

  .csv(az(s"lags/animals/${threshold}/lags/mouse_animal_lags.tsv"))

  cow_animal_lags.coalesce(1).write.option("sep","\t").option("header","true")

  .csv(az(s"lags/animals/${threshold}/lags/cow_animal_lags.tsv"))


  /* ... new cell ... */

  def processLiver(df: DataFrame) = {

    df.na.fill("")

      .withColumn("liver_TPM", toDouble($"liver")).drop("liver")

      .withColumn("ident", toDouble($"identity")).drop("identity")

      .withColumn("aligment_len", toLong($"aligment_length")).drop("aligment_length")

      .withColumn("start_q", toLong($"start_query")).drop("start_query")

      .withColumn("end_q", toLong($"end_query")).drop("end_query")

      .withColumn("start_t", toLong($"start_target")).drop("start_target")

      .withColumn("end_t", toLong($"end_target")).drop("end_target")

      .withColumn("_tmp", split($"genage", "\\|"))

      .select(

        genAgeExtract($"_tmp".getItem(0)).as("GenAge ID"),

        $"transcript",

        $"liver_TPM",

        $"ident",

        $"aligment_len",

        $"mismatches",

        $"gaps",

        $"start_q",

        $"end_q",

        $"start_t",

        $"end_t"

      ).drop("_tmp").drop($"genage")

      .sort($"liver_TPM".desc)

  }

  

  def processKidney(df: DataFrame) = {

    df.na.fill("")

      .withColumn("kidney_TPM", toDouble($"kidney")).drop("kidney")

      .withColumn("ident", toDouble($"identity")).drop("identity")

      .withColumn("aligment_len", toLong($"aligment_length")).drop("aligment_length")

      .withColumn("start_q", toLong($"start_query")).drop("start_query")

      .withColumn("end_q", toLong($"end_query")).drop("end_query")

      .withColumn("start_t", toLong($"start_target")).drop("start_target")

      .withColumn("end_t", toLong($"end_target")).drop("end_target")

      .withColumn("_tmp", split($"genage", "\\|"))

      .select(

        genAgeExtract($"_tmp".getItem(0)).as("GenAge ID"),

        $"transcript",

        $"kidney_TPM",

        $"ident",

        $"aligment_len",

        $"mismatches",

        $"gaps",

        $"start_q",

        $"end_q",

        $"start_t",

        $"end_t"

      ).drop("_tmp").drop($"genage")

      .sort($"kidney_TPM".desc)

  }

  

  val lags_liver_columns: List[String] = lags_models_columns ++ results_columns.tail.filterNot(s=>s.contains("kidney") || s.contains("avg_expression"))

  val lags_kidney_columns: List[String] = lags_models_columns ++ results_columns.tail.filterNot(s=>s.contains("liver") || s.contains("avg_expression"))

  def join_liver_with_models(df: DataFrame) = {

    lags_models.join(df, "GenAge ID").select(lags_liver_columns.head, lags_liver_columns.tail:_*)

  }

  

  def join_kidney_with_models(df: DataFrame) = {

    lags_models.join(df, "GenAge ID").select(lags_kidney_columns.head, lags_kidney_columns.tail:_*)

  }

  

  def process_with_liver_models(df: DataFrame) = join_liver_with_models( processLiver(df))

  def process_with_kidney_models(df: DataFrame) = join_kidney_with_models( processKidney(df))


  /* ... new cell ... */

  val bowhead_liver_animal_lags = process_with_liver_models(bowhead_liver_lags_exp).join(extra, $"Gene ID"===$"GenAge ID").drop($"Gene ID")

  val bowhead_kidney_animal_lags = process_with_kidney_models(bowhead_kidney_lags_exp).join(extra, $"Gene ID"===$"GenAge ID").drop($"Gene ID")

  /* ... new cell ... */

  bowhead_liver_animal_lags.coalesce(1).write.option("sep","\t").option("header","true").csv(az(s"lags/animals/${threshold}/lags/bowhead_liver_animal_lags.tsv"))

  bowhead_kidney_animal_lags.coalesce(1).write.option("sep","\t").option("header","true").csv(az(s"lags/animals/${threshold}/lags/bowhead_kidney_animal_lags.tsv"))
}
                  