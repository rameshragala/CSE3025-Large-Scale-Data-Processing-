import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SequenceFileReaderMapper extends Mapper<Text,Text,Text,Text> {
	
	/**
	 * This is the map function, it does not perform much functionality.
	 * It only writes key and value pair to the context
	 * which will then be written into the sequence file.
	 */
	//@Override
    protected void map(Text key, Text value,Context context) throws IOException, InterruptedException {
            context.write(key, value);                
    }
	
}
