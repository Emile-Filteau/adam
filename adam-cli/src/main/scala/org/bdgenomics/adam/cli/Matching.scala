package org.bdgenomics.adam.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Matching extends BDGCommandCompanion {
  val commandName = "matching"
  val commandDescription = "Finds all variants with a given rsid"

  def apply(cmdLine: Array[String]) = {
    new Matching(Args4j[MatchingArgs](cmdLine))
  }
}

class MatchingArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM or FASTA file to match variants from", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "The path to save result to", index = 1)
  var outputPath: String = null
  @Argument(required = true, metaVar = "RSID", usage = "The rsid that you want to match against the database", index = 2)
  var rsid: String = null
}

class Matching(protected val args: MatchingArgs) extends BDGSparkCommand[MatchingArgs] with Logging {
  val companion = Matching

  def run(sc: SparkContext) {

    // Read from disk
    val genotypes = sc.loadGenotypes(args.inputPath)

    // Filter variants by RSID
    val filteredVariants = genotypes.filterVariants(args.rsid)

    // Print the first result
    filteredVariants.take(1).foreach(println)

    // Save to text file
    filteredVariants.saveAsTextFile(args.outputPath)
  }
}
