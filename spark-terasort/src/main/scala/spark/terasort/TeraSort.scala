/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.terasort

import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}


object TeraSort {

    implicit val caseInsensitiveOrdering = UnsignedBytes.lexicographicalComparator

    def main(args: Array[String]) {



        if (args.length < 5) {
            println("Usage:")
            println("spark-submit " +
              "XXX " +
              "[input-file] [output-file] [partition num] [number of sampling records][ number of sampling thread]")
            println(" ")
            println("when partition num and size of sample are negative numbers, the program would take default values")
            println("Example:")
            println("spark-submit " +
              "spark.terasort.TeraSort " +
              "spark-terasort-1.0-SNAPSHOT-with-dependencies.jar " +
              "/home/terasort_in /home/terasort_out 4096 32 -1")
            System.exit(0)
        }


        val conf = new SparkConf()
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .setAppName(s"TeraSort")
          .set("spark.hadoop.dfs.replication", "1")

        // Process command line arguments

        val inputFile = args(0)
        val outputFile = args(1)
        val partitionNum = args(2).toInt
        val sampleNum = args(3).toLong
        val threadNum = args(4).toInt

        val sc = new SparkContext(conf)


        val hadoopConf = new JobConf()
        hadoopConf.set("mapreduce.input.fileinputformat.inputdir", inputFile)
        val partitionFile = new Path(outputFile,
            SortInputFormat.PARTITION_FILENAME)
        val jobContext = Job.getInstance(hadoopConf)
        SortInputFormat.writePartitionFile(jobContext, partitionNum, sampleNum, partitionFile, threadNum)

        val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], SortInputFormat](inputFile)
//        val sorted = dataset.repartitionAndSortWithinPartitions(new TeraRangePartitioner(dataset, partitionNum, true,
//            jobContext, hadoopConf, partitionFile))

        val sorted = dataset.repartitionAndSortWithinPartitions(new TrieRangePartitioner(dataset, partitionNum, true,
            jobContext, hadoopConf, partitionFile))






        sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)
        sc.stop()

    }

}

