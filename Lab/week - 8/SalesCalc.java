import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SalesCalc extends Configured implements Tool {    
    enum Sales {
        SALES_DATA_MISSING
    }
    // Mapper
    public static class SalesMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text item = new Text();
        IntWritable sales = new IntWritable();
        public void map(LongWritable key, Text value, Context context) 
                 throws IOException, InterruptedException {
             // Splitting the line on tab
             String[] salesArr = value.toString().split("\t");
             item.set(salesArr[0]);
                
             if(salesArr[1] != null && !salesArr[1].trim().equals("")) {
                 sales.set(Integer.parseInt(salesArr[1]));
             }else {
                 // incrementing counter
                 context.getCounter(Sales.SALES_DATA_MISSING).increment(1);
                 sales.set(0);
             }
            
             context.write(item, sales);
         }
    }
    
    // Reducer
    public static class TotalSalesReducer extends Reducer<Text, Text, Text, IntWritable>{
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }      
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        int exitFlag = ToolRunner.run(new SalesCalc(), args);
        System.exit(exitFlag);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "SalesCalc");
        job.setJarByClass(getClass());
        job.setMapperClass(SalesMapper.class);    
        job.setReducerClass(TotalSalesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

