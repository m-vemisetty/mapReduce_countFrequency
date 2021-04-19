package mr_demo.CrimeFrequency;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class CrimeCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override public void reduce( Text eventID,  Iterable<IntWritable> counts,  Context context)
	         throws IOException,  InterruptedException {

	      int sum  = 0;
	      for ( IntWritable count  : counts) {
	        sum  += count.get();
	      }
	      context.write(eventID,  new IntWritable(sum));
	    }
}
