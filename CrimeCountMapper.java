package mr_demo.CrimeFrequency;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	  @Override
	  public void map(LongWritable offset, Text lineText, Context context)
	      throws IOException, InterruptedException {

	    String line = lineText.toString();
	    String eventID = line.split(",")[1];
	    context.write(new Text(eventID), one);
	  }
}

