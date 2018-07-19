/*
%AddDeps org.bdgenomics.adam adam-core-spark2_2.11 0.24.0
%AddDeps comp.bio.aging adam-playground_2.11 0.0.12 --repository https://dl.bintray.com/comp-bio-aging/main/

%AddDeps org.apache.hadoop hadoop-azure 2.7.6
%AddDeps com.microsoft.azure azure-storage 2.0.0
*/

import ammonite.ops._
import coursier.maven.MavenRepository
import org.apache.spark.sql.{ColumnName, DataFrame, Dataset}
interp.repositories() ++= Seq(MavenRepository("https://dl.bintray.com/comp-bio-aging/main/"))

import $ivy.`org.apache.spark:spark-core_2.11:2.3.1`
import $ivy.`org.apache.spark:spark-sql_2.11:2.3.1`
import $ivy.`org.apache.spark:spark-mllib_2.11:2.3.1`
import $ivy.`org.apache.spark:spark-mllib-local_2.11:2.3.1`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`comp.bio.aging:adam-playground_2.11:0.0.13`


/*
%AddDeps comp.bio.aging adam-playground_2.11 0.0.13 --transitive --repository https://dl.bintray.com/comp-bio-aging/main/

%AddDeps org.apache.hadoop hadoop-azure 2.7.6
%AddDeps com.microsoft.azure azure-storage 2.0.0
*/
object Cells {
  import  org.apache.spark._
  import org.apache.spark.sql._
  import org.apache.spark.sql.types.StructType
  import scala.reflect.runtime.universe._
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.rdd._
  import org.apache.spark.ml.linalg._
  import org.apache.spark.ml.stat._

  //import comp.bio.aging.playground.extensions._
  import org.bdgenomics.adam.rdd.ADAMContext._

  def sparkHadoopConf(sc: SparkContext, acountName: String, accountKey: String) = {
    sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.azure.account.key." + acountName + ".blob.core.windows.net", accountKey)
    sc
  }

  def azurize(container: String, accountName: String, blobFile: String): String = "wasbs://"+container+"@"+accountName+".blob.core.windows.net/"+blobFile

  val account = "pipelines1"
  val key = "JHVfH9TNAcLhbIjVmIzT387Z2eAOFz1T0xvzJBb7z3jbWtMXspZD+E87OBDIvOyvnd8OWMdyPfKYSboGMKfIxQ=="
  val connString = s"DefaultEndpointsProtocol=https;AccountName=pipelines1;AccountKey=${key};EndpointSuffix=core.windows.net"
  def az(path: String): String = azurize("storage", account, path)

  def write(df: DataFrame, url: String, header: Boolean = true) = {
    df.coalesce(1).write.option("sep","\t").option("header", header).csv(url)
    url
  }

  val sparkContext: SparkContext = sc
  sparkHadoopConf(sparkContext, account, key)

  val spark = SparkSession
    .builder()
    .appName("whales_validation")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val toDouble = udf[Double, String]( _.toDouble)

  def readTSV(path: String, header: Boolean = false, sep: String = "\t", infer: Boolean = true): DataFrame = spark.read
    .option("sep", sep)
    .option("comment", "#")
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .option("inferSchema", infer)
    .option("header", header)
    .option("maxColumns", 150000)
    .csv(path)

  def readTypedTSV[T <: Product](path: String, header: Boolean = false, sep: String = "\t")
                                (implicit tag: TypeTag[T]): Dataset[T] = {
    implicit val encoder: StructType = Encoders.product[T](tag).schema
    spark.read
      .option("sep", sep)
      .option("comment", "#")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("header", header)
      .schema(encoder)
      .csv(path).as[T]
  }

  def toVectors(dataFrame: DataFrame, columns: Seq[String], output: String) = {
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.linalg.Vectors

    val assembler = new VectorAssembler()
      .setInputCols(columns.toArray)
      .setOutputCol(output)

    assembler.transform(dataFrame.na.fill(0.0, columns).na.fill("")).select(output)
  }

  def byGo(df: DataFrame, species: String) =
  {
    df.flatMap{
      case row =>

        Seq(row)
    }
  }

  val expressionsPath = "expressions"
  val byGoPath = expressionsPath + "/go"
  val comparison = byGoPath + "/gray_whale_with_bowhead_with_minke_with_NMR_with_human_with_mouse_with_cow_full_outer_counts_extended.tsv"
  val grouped = byGoPath + "/grouped"
  val ranked = byGoPath + "/grouped/ranked"


  val goTable = readTSV(az(comparison), true)

  //%%dataframe goTable

  val features = List("gray_whale_liver", "bowhead_whale_liver", "minke_liver", "NMR_liver", "human_liver", "mouse_totalRNA_liver", "mouse_mRNA_liver", "cow_totalRNA_liver", "cow_mRNA_liver", "gray_whale_kidney", "bowhead_whale_kidney", "minke_kidney", "NMR_kidney", "human_kidney", "mouse_totalRNA_kidney", "mouse_mRNA_kidney", "cow_totalRNA_kidney", "cow_mRNA_kidney")

  val vec = toVectors(goTable, features, "features").cache
  vec

  //%%dataframe vec

  val cor =   Correlation.corr(vec, "features")

  val components = goTable.where("namespace = 'cellular_component'")
  val processes = goTable.where("namespace = 'biological_process'")
  val functions = goTable.where("namespace = 'molecular_function'")

  (components.count, processes.count, functions.count, goTable.count)

  write(components, az(grouped + "/by_cellular_component.tsv"))


  write(processes, az(grouped + "/by_biological_process.tsv"))

  write(functions, az(grouped + "/by_molecular_function.tsv"))

  import org.apache.spark.sql.{ColumnName, Dataset}
  import org.apache.spark.sql.expressions.Window

  def rank(df: DataFrame, name: String, rankSuffix: String = "_rank") =
    df.withColumn(name + rankSuffix, org.apache.spark.sql.functions.dense_rank().over(Window.orderBy(new ColumnName(name).desc)))

  def ranks(df: DataFrame,
            names: Seq[String],
            rankSuffix: String = "_rank") = names.foldLeft(df){
    case (f, n)=> rank(f, n, rankSuffix)
  }

  val rankedCols = components.columns.drop(11)++List("gray_whale_avg")

  val rankedComponents = ranks(components, rankedCols)
  val rankedProcesses = ranks(processes, rankedCols)
  val rankedFunctions = ranks(functions, rankedCols)

  write(rankedComponents, az(grouped + "/by_cellular_component_ranked.tsv"))
  write(rankedProcesses, az(grouped + "/by_biological_process_ranked.tsv"))
  write(rankedFunctions, az(grouped + "/by_molecular_function_ranked.tsv"))

  //%%dataframe rankedComponents

  import org.apache.spark.storage._

  val mapping =  readTSV(az("/indexes/uniprot/idmapping_selected.tab"))
    .toDF("uniprot_ac","uniprot_id","entrez","refSeq", "gi","pdb", "go", "uniref100", "uniref90", "uniref50", "uniparc", "pir", "taxon", "mim", "unigene", "pubmed", "embl", "embl_cds", "ensembl", "ensembl_trs", "ensembl_pro", "additional_pubmed")
    .persist(StorageLevel.MEMORY_AND_DISK)

  //%%dataframe mapping

  def convertCorrellationMatrix(matrix: Matrix, columns: Seq[String]) = {
    require(columns.size == matrix.numCols)
    for(r <- 0 until matrix.numRows) yield {
      val seq = for(c <- 0 until matrix.numCols) yield matrix(r, c)
      Row.fromSeq(columns(r)::seq.toList)
    }
  }

  import org.apache.spark.sql.types._

  def doublesByColumns(columns: Seq[String]) = columns.map(c=>StructField(c, DoubleType, false)).toList

  def transformCorrellationMatrix(dataFrame: DataFrame, columns: Seq[String])(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val rows  = dataFrame.rdd
      .flatMap{ case Row(matrix: Matrix) => convertCorrellationMatrix(matrix, columns) }
    sparkSession.createDataFrame(rows, StructType(StructField("feature", StringType, false)::doublesByColumns(columns)))
  }

  def pearsonCorrellation(dataFrame: DataFrame, columns: Seq[String])(implicit sparkSession: SparkSession) = {
    val cor = toVectors(dataFrame, columns, "features")
    val df = Correlation.corr(cor, "features")
    transformCorrellationMatrix(df, columns)
  }

  def spearmanCorrellation(dataFrame: DataFrame, columns: Seq[String])(implicit sparkSession: SparkSession) = {
    val cor = toVectors(dataFrame, columns.toSeq, "features").persist(StorageLevel.MEMORY_AND_DISK)
    import org.apache.spark.ml.linalg.Matrix
    val df = Correlation.corr(cor, "features", method = "spearman")
    transformCorrellationMatrix(df, columns)
  }

  val corPath = grouped + "/correlations"

  val p = pearsonCorrellation(goTable, features)(spark)

  write(p, az(corPath + "/pearson_transcripts.tsv"))

  val pCorComponents = pearsonCorrellation(components, features)(spark)
  val pCorFunctions =  pearsonCorrellation(functions, features)(spark)
  val pCorProcesses = pearsonCorrellation(processes, features)(spark)

  write(pCorComponents, az(corPath + "/pearson_components.tsv"))
  write(pCorFunctions, az(corPath + "/pearson_functions.tsv"))
  write(pCorProcesses, az(corPath + "/pearson_proccesses.tsv"))

  val sCorComponents = spearmanCorrellation(components, features)(spark)
  val sCorFunctions =  spearmanCorrellation(functions, features)(spark)
  val sCorProcesses = spearmanCorrellation(processes, features)(spark)

  write(sCorComponents, az(corPath + "/spearman_components.tsv"))
  write(sCorFunctions, az(corPath + "/spearman_functions.tsv"))
  write(sCorProcesses, az(corPath + "/spearman_proccesses.tsv"))

  //%%dataframe p

  val validationPath = expressionsPath + "/" + "validation"
  val mouseValidationPath = validationPath + "/" + "mouse"
  val jenageValidationPath = mouseValidationPath + "/" + "GSE75192"
  val mouseColsValidationPath = jenageValidationPath + "/" + "expressions_columns_GSE75192.tsv"
  ///storage/expressions/validation/mouse


  val mouseCols = readTSV(az(mouseColsValidationPath), true)

  val mouseSamples = mouseCols.columns.toList.tail
  mouseSamples

  val mouseCorComponents = spearmanCorrellation(mouseCols, mouseSamples)(spark)

  //%%dataframe mouseCorComponents

  val mouseCorPearsonComponents = pearsonCorrellation(mouseCols, mouseSamples)(spark)

  //%%dataframe mouseCorPearsonComponents

  mouseCols.count


  val go = "GO:0070062" //put your GO term inside quotes
  val searchGO = mapping.filter($"go".contains(go))

  //%%dataframe searchGO

  val uniref90: String = "UniRef90_Q6GZX4" //put your Uniref90 term inside quotes
  val searchUniref90 = mapping.filter($"uniref90".contains(uniref90))

  //%%dataframe searchUniref90



}