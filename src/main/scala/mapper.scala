package co.vine.sre

import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import scala.collection.JavaConverters._

class UniqMapper extends Mapper[LongWritable, Text, Text, NullWritable] {
  private val empty = NullWritable.get()

  type MapperContext = Mapper[LongWritable, Text, Text, NullWritable]#Context

  override def map(key: LongWritable, value: Text, context: MapperContext):Unit = {
      context.write(value, empty)
  }
}