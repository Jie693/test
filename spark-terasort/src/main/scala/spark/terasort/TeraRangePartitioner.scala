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
import java.util

import org.apache.hadoop.io.WritableUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.hadoop.mapreduce.JobContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{DeserializationStream, JavaSerializer, SerializationStream, SerializerInstance}
import org.apache.spark.{Partitioner, SparkEnv}

import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

class TeraRangePartitioner[K: Ordering : ClassTag, V](
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
    private var rangeBounds: Array[K] = TeraRangePartitioner.readPartitions(partFile.getFileSystem(conf),
        partFile, conf, partitions).asInstanceOf[Array[K]];


    def numPartitions: Int = rangeBounds.length + 1

    private var binarySearch: ((Array[K], K) => Int) = TeraRangePartitioner.makeBinarySearch[K]

    @throws(classOf[IOException])
    private def writeObject(out: ObjectOutputStream): Unit = TeraRangePartitioner.tryOrIOException {
        val sfactory = SparkEnv.get.serializer
        sfactory match {
            case js: JavaSerializer => out.defaultWriteObject()
            case _ =>
                out.writeBoolean(ascending)
                out.writeObject(ordering)
                out.writeObject(binarySearch)

                val ser = sfactory.newInstance()
                TeraRangePartitioner.serializeViaNestedStream(out, ser) { stream =>
                    stream.writeObject(scala.reflect.classTag[Array[K]])
                    stream.writeObject(rangeBounds)
                }
        }
    }

    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit = TeraRangePartitioner.tryOrIOException {
        val sfactory = SparkEnv.get.serializer
        sfactory match {
            case js: JavaSerializer => in.defaultReadObject()
            case _ =>
                ascending = in.readBoolean()
                ordering = in.readObject().asInstanceOf[Ordering[K]]
                binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

                val ser = sfactory.newInstance()
                TeraRangePartitioner.deserializeViaNestedStream(in, ser) { ds =>
                    implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
                    rangeBounds = ds.readObject[Array[K]]()
                }
        }
    }


    def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[K]
        var partition = 0
        partition = binarySearch(rangeBounds, k)
        // binarySearch either returns the match location or -[insertion point]-1
        if (partition < 0) {
            partition = -partition - 1
        }
        if (partition > rangeBounds.length) {
            partition = rangeBounds.length
        }
        //        }
        if (ascending) {
            partition
        } else {
            rangeBounds.length - partition
        }
        partition
    }

    override def equals(other: Any): Boolean = other match {
        case r: TeraRangePartitioner[_, _] =>
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


}

object TeraRangePartitioner {

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

    def makeBinarySearch[K: Ordering : ClassTag]: (Array[K], K) => Int = {
        // For primitive keys, we can use the natural ordering. Otherwise, use the Ordering comparator.
        classTag[K] match {
            case ClassTag.Float =>
                (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Float]], x.asInstanceOf[Float])
            case ClassTag.Double =>
                (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Double]], x.asInstanceOf[Double])
            case ClassTag.Byte =>
                (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Byte]], x.asInstanceOf[Byte])
            case ClassTag.Char =>
                (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Char]], x.asInstanceOf[Char])
            case ClassTag.Short =>
                (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Short]], x.asInstanceOf[Short])
            case ClassTag.Int =>
                (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Int]], x.asInstanceOf[Int])
            case ClassTag.Long =>
                (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Long]], x.asInstanceOf[Long])
            case _ =>
                val comparator = implicitly[Ordering[K]].asInstanceOf[java.util.Comparator[Any]]
                (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[AnyRef]], x.asInstanceOf[AnyRef], comparator)
        }
    }

    @throws[IOException]
    def readPartitions(fs: FileSystem, p: Path, conf: Configuration, partitions: Int): Array[Array[Byte]] = {
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

