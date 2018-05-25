/*
%AddDeps org.bdgenomics.adam adam-core-spark2_2.11 0.24.0
%AddDeps comp.bio.aging adam-playground_2.11 0.0.12 --repository https://dl.bintray.com/comp-bio-aging/main/

%AddDeps org.apache.hadoop hadoop-azure 2.7.6
%AddDeps com.microsoft.azure azure-storage 7.0.0
*/
import ammonite.ops._
import coursier.maven.MavenRepository
interp.repositories() ++= Seq(MavenRepository("https://dl.bintray.com/comp-bio-aging/main/"))

import $ivy.`org.apache.spark:spark-core_2.11:2.3.0`
import $ivy.`org.apache.spark:spark-sql_2.11:2.3.0`
import $ivy.`org.apache.spark:spark-mllib_2.11:2.3.0`
//import $ivy.`comp.bio.aging:adam-playground_2.11:0.0.12`

import  org.apache.spark._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.types.StructType
import scala.reflect.runtime.universe._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd._


def sparkHadoopConf(sc: SparkContext, acountName: String, accountKey: String) = {
  sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
  sc.hadoopConfiguration.set("fs.azure.account.key." + acountName + ".blob.core.windows.net", accountKey)
  sc
}

def azurize(container: String, accountName: String, blobFile: String): String = "wasbs://"+container+"@"+accountName+".blob.core.windows.net/"+blobFile

val connString = ""
val account = "pipelines1"
val key = ""
def az(path: String): String = azurize("storage", account, path)

def write(df: DataFrame, url: String, header: Boolean = true) = {
  df.coalesce(1).write.option("sep","\t").option("header", header).csv(url)
  url
}

val sparkContext = sc
sparkHadoopConf(sparkContext, account, key)
  
val spark = SparkSession
  .builder()
  .appName("mapping_models")
  .getOrCreate()

import org.apache.spark.sql.functions._
import spark.implicits._

val toDouble = udf[Double, String]( _.toDouble)

def readTSV(path: String, header: Boolean = false, sep: String = "\t"): DataFrame = spark.read
    .option("sep", sep)
    .option("comment", "#")
    .option("inferSchema", true)
    .option("header", header)
    .option("ignoreLeadingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .option("ignoreTrailingWhiteSpace", true)
    .option("maxColumns", 150000)
    .csv(path)

val expressionsPath = az("expressions")
val byGoPath = expressionsPath + "/go"
val comparison = byGoPath + "/gray_whale_with_bowhead_with_minke_with_NMR_with_human_with_mouse_with_cow_full_outer_counts_extended.tsv"

//val txtPath = az(gtexPath + "/GTEx_Analysis_2016-01-15_v7_RSEMv1.2.22_transcript_tpm.txt")
//val gctPath = az(gtexPath + "/GTEx_Analysis_2016-01-15_v7_RNASeQCv1.1.8_gene_tpm.gct")


val goTable = readTSV(comparison, true)


val components = goTable.where($"namespace" === "cellular_component")
val processes = goTable.where($"namespace" === "biological_process")
val functions = goTable.where($"namespace" === "molecular_function")


val other = goTable.where($"namespace" =!= "cellular_component" && $"namespace" =!= "biological_process" && $"namespace" =!= "molecular_function")
(components.count, processes.count, functions.count, other.count, goTable.count)

val grouped = byGoPath + "/grouped"
val ranked = byGoPath + "/grouped/ranked"

write(components, az(grouped + "/by_cellular_component.tsv"))
write(processes, az(grouped + "/by_biological_process.tsv"))
write(functions, az(grouped + "/by_molecular_function.tsv"))


