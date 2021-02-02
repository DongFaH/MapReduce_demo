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
 * @ClassName: IP
 * @Author: HongLiang
 * @Description: TODO
 * @Date: Create in 22:06 2021/2/1
 * @Version: 1.0
 * @Modified By:HongLiang
 */

class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    Text key = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Parser parser = new Parser(value.toString());
        String ip = parser.getRemote_addr();
        if (parser.getValid() && ip != null) {
            this.key.set(ip);
            context.write(this.key, one);
        }
    }
}

class IPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : value) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

public class IP {
    public Job main() throws IOException {
        Configuration configuration = new SingleConfiguration().getConfiguration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(IP.class);

        job.setMapperClass(IPMapper.class);
        job.setCombinerClass(IPReducer.class);
        job.setReducerClass(IPReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }
}
