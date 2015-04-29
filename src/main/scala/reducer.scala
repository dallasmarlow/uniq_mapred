package co.vine.sre

import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.hadoop.io.{NullWritable, Text}
import scala.collection.JavaConversions._

class UniqReducer extends Reducer[Text, NullWritable, Text, NullWritable] {
  val empty = NullWritable.get()

  type ReducerContext = Reducer[Text, NullWritable, Text, NullWritable]#Context

  override def reduce(key: Text, values: java.lang.Iterable[NullWritable], context: ReducerContext) {
      context.write(key, empty)
  }
}