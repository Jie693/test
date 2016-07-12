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

import java.io._

import TrieRangePartitioner.TrieNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{WritableComparator, WritableUtils}
import org.apache.hadoop.mapreduce.JobContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{DeserializationStream, JavaSerializer, SerializationStream, SerializerInstance}
import org.apache.spark.{Partitioner, SparkEnv}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks._
import scala.util.control.NonFatal

class TrieRangePartitioner[K: Ordering : ClassTag, V](
                                                       rdd: RDD[_ <: Product2[K, V]],
                                                       var partitions: Int,
                                                       private var ascending: Boolean = true,
                                                       val job: JobContext,
                                                       val conf: Configuration,
                                                       val partFile: Path
                                                     )
  extends Partitioner {
    if (partitions < 0) {
        partitions = rdd.partitions.length
    }

    // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
    require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

    private var ordering = implicitly[Ordering[K]]
    // An array of upper bounds for the first (partitions - 1) partitions

    private var rangeBounds: Array[K] = TrieRangePartitioner.readPartitions(partFile.getFileSystem(conf),
        partFile, conf, partitions).asInstanceOf[Array[K]];


    private var trie: TrieNode = null


    override def equals(other: Any): Boolean = other match {
        case r: TrieRangePartitioner[_, _] =>
            r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
        case _ =>
            false
    }

    override def hashCode(): Int = {
        val prime = 31
        var result = 1
        var i = 0
        while (i < rangeBounds.length) {
            result = prime * result + rangeBounds(i).hashCode
            i += 1
        }
        result = prime * result + ascending.hashCode
        result
    }

    def numPartitions: Int = rangeBounds.length + 1


    @throws(classOf[IOException])
    private def writeObject(out: ObjectOutputStream): Unit = TrieRangePartitioner.tryOrIOException {
        val sfactory = SparkEnv.get.serializer
        sfactory match {
            case js: JavaSerializer => out.defaultWriteObject()
            case _ =>
                out.writeBoolean(ascending)
                out.writeObject(ordering)

                val ser = sfactory.newInstance()
                TrieRangePartitioner.serializeViaNestedStream(out, ser) { stream =>
                    stream.writeObject(scala.reflect.classTag[Array[K]])
                    stream.writeObject(rangeBounds)
                }
        }
    }

    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit = TrieRangePartitioner.tryOrIOException {
        val sfactory = SparkEnv.get.serializer
        sfactory match {
            case js: JavaSerializer => in.defaultReadObject()
            case _ =>
                ascending = in.readBoolean()
                ordering = in.readObject().asInstanceOf[Ordering[K]]

                val ser = sfactory.newInstance()
                TrieRangePartitioner.deserializeViaNestedStream(in, ser) { ds =>
                    implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
                    rangeBounds = ds.readObject[Array[K]]()
                }
                this.trie = TrieRangePartitioner.buildTrie(rangeBounds.asInstanceOf[Array[Array[Byte]]],
                    0, rangeBounds.length, new ArrayBuffer[Byte](), 2)
        }
    }


    def getPartition(key: Any): Int = {
        val partition = trie.findPartition(key.asInstanceOf[Array[Byte]])
        partition
    }


}

object TrieRangePartitioner {

    abstract private[terasort] class TrieNode(var level: Int) {
        def findPartition(key: Array[Byte]): Int

        def getLevel: Int = level
    }

    private[terasort] class InnerTrieNode(level: Int) extends TrieNode(level) {
        private val child: Array[TrieNode] = new Array[TrieNode](256)

        def findPartition(key: Array[Byte]): Int = {
            val level: Int = getLevel
            if (key.size <= level) {
                child(0).findPartition(key)
            } else {
                child(key(level) & 0xff).findPartition(key)
            }
        }

        def getChild(): Array[TrieNode] = child

        def setChild(idx: Int, child: TrieNode) {
            this.child(idx) = child
        }
    }


    private[terasort] class LeafTrieNode(level: Int,
                                         var splitPoints: Array[Array[Byte]],
                                         var lower: Int, var upper: Int) extends TrieNode(level) {
        def findPartition(key: Array[Byte]): Int = {
            var i: Int = lower
            var result: Int = upper
            breakable {
                while (i < upper) {
                    val currentSplitPoint = splitPoints(i)
                    if (WritableComparator.compareBytes(currentSplitPoint, 0, currentSplitPoint.length, key, 0, key.length) >= 0) {
                        result = i
                        break;
                    }
                    i += 1
                }
            }
            result
        }

    }

    private def buildTrie(splits: Array[Array[Byte]], lower: Int, upper: Int, prefix: ArrayBuffer[Byte], maxDepth: Int): TrieNode = {
        val depth: Int = prefix.size
        if (depth >= maxDepth || lower == upper) {
            return new LeafTrieNode(depth, splits, lower, upper)
        }
        val result: InnerTrieNode = new InnerTrieNode(depth)
        result.getChild()

        val trial: ArrayBuffer[Byte] = new ArrayBuffer[Byte]()
        trial ++= prefix
        trial += 0.toByte
        var lowerVar = lower
        var currentBound: Int = lower
        var ch: Int = 0
        while (ch < 255) {
            {
                trial(depth) = (ch + 1).toByte
                lowerVar = currentBound
                breakable {
                    while (currentBound < upper) {
                        {
                            val cutpoint: Array[Byte] = splits(currentBound)
                            if (WritableComparator.compareBytes(cutpoint, 0, cutpoint.size, trial.toArray, 0, trial.size) >= 0) {
                                break
                            }
                            currentBound += 1
                        }
                    }
                }
                trial(depth) = ch.toByte
                result.getChild()(ch) = buildTrie(splits, lowerVar, currentBound, trial, maxDepth)
            }
            ch += 1
        }
        trial(depth) = 255.toByte
        result.getChild()(255) = buildTrie(splits, currentBound, upper, trial, maxDepth)
        result
    }


    def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
      f: SerializationStream => Unit): Unit = {
        val osWrapper = ser.serializeStream(new OutputStream {
            override def write(b: Int): Unit = os.write(b)

            override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
        })
        try {
            f(osWrapper)
        } finally {
            osWrapper.close()
        }
    }


    /** Deserialize via nested stream using specific serializer */
    def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
      f: DeserializationStream => Unit): Unit = {
        val isWrapper = ser.deserializeStream(new InputStream {
            override def read(): Int = is.read()

            override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
        })
        try {
            f(isWrapper)
        } finally {
            isWrapper.close()
        }
    }

    @throws[IOException]
    def readPartitions(fs: FileSystem, p: Path, conf: Configuration, partitions: Int): Array[Array[Byte]] = TrieRangePartitioner.tryOrIOException {
        val result: Array[Array[Byte]] = new Array[Array[Byte]](partitions - 1)
        val reader: DataInputStream = fs.open(p)
        var i: Int = 0
        while (i < partitions - 1) {
            {
                val newLength: Int = WritableUtils.readVInt(reader)
                result(i) = new Array[Byte](newLength)
                reader.readFully(result(i), 0, newLength)
            }
            i += 1
        }
        reader.close
        result
    }


    /**
      * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
      * exceptions as IOException.
      */
    def tryOrIOException[T](block: => T): T = {
        try {
            block
        } catch {
            case e: IOException =>
                //                logError("Exception encountered", e)
                throw e
            case NonFatal(e) =>
                //                logError("Exception encountered", e)
                throw new IOException(e)
        }
    }

}

