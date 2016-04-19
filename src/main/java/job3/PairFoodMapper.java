package job3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PairFoodMapper  extends Mapper<LongWritable, Text, BiKeyWritable, BiItemWritable>{

}
