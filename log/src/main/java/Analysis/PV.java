package Analysis;

import Conf.SingleConfiguration;
import LogParser.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: PV
 * @Author: HongLiang
 * @Description: TODO
 * @Date: Create in 22:05 2021/2/1
 * @Version: 1.0
 * @Modified By:HongLiang
 */
class PVMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    Text key = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Parser parser = new Parser(value.toString());
        String page = parser.getPage();
        if (parser.getValid() && page != null) {
            this.key.set(page);
            context.write(this.key, one);
        }
    }
}

class PVReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : value) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

public class PV {
    public Job main() throws IOException {
        Configuration configuration = new SingleConfiguration().getConfiguration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(PV.class);

        job.setMapperClass(PVMapper.class);
        job.setCombinerClass(PVReducer.class);
        job.setReducerClass(PVReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }
}
