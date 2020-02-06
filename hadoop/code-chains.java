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
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class StockTrans extends Configured implements Tool{
    // Mapper 1
    public static class StockFieldMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text symbol = new Text();
       
        public void map(LongWritable key, Text value, Context context)
                 throws IOException, InterruptedException {
             // Splitting the line on tab
             String[] stringArr = value.toString().split("\t");
             //Setting symbol and transaction values
             symbol.set(stringArr[0]);
             Integer trans = Integer.parseInt(stringArr[2]);
             context.write(symbol, new IntWritable(trans));
         }
    }
   
    // Mapper 2
    public static class UpperCaseMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
        public void map(Text key, IntWritable value, Context context)
                 throws IOException, InterruptedException {
       
            String symbol = key.toString().toUpperCase();
             
            context.write(new Text(symbol), value);
         }
    }
   
    // Reduce function
    public static class TotalTransReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
       
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
        int exitFlag = ToolRunner.run(new StockTrans(), args);
        System.exit(exitFlag);
 
    }
   
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock transactio");
        job.setJarByClass(getClass());
        // MapReduce chaining
        Configuration map1Conf = new Configuration(false);
        ChainMapper.addMapper(job, StockFieldMapper.class, LongWritable.class, Text.class,
                   Text.class, IntWritable.class,  map1Conf);
       
        Configuration map2Conf = new Configuration(false);
        ChainMapper.addMapper(job, UpperCaseMapper.class, Text.class, IntWritable.class,
                   Text.class, IntWritable.class, map2Conf);
       
        Configuration reduceConf = new Configuration(false);        
        ChainReducer.setReducer(job, TotalTransReducer.class, Text.class, IntWritable.class,
                 Text.class, IntWritable.class, reduceConf);
 
        ChainReducer.addMapper(job, InverseMapper.class, Text.class, IntWritable.class,
                 IntWritable.class, Text.class, null);
         
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
