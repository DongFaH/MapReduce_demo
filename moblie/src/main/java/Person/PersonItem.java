package Person;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import Conf.SingleConfiguration;
/**
 * @ClassName: Person
 * @Author: HongLiang
 * @Description: TODO
 * @Date: Create in 20:59 2021/1/28
 * @Version: 1.0
 * @Modified By:HongLiang
 */

class PersonItemMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable one = new IntWritable(1);
    private Text key = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(",");
        this.key.set(strings[0]);
        context.write(this.key, one);
    }
}

class PersonItemReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

public class PersonItem {


    public Job main() throws IOException {
        //1. 获取Job 的对象实例

        Configuration configuration = new SingleConfiguration().getConfiguration();
        Job job = Job.getInstance(configuration);

        // 2. 设置jar 的主类
        job.setJarByClass(PersonItem.class);

        // 3. 设置map和reducer的主类
        job.setMapperClass(PersonItemMapper.class);
        job.setCombinerClass(PersonItemReducer.class);
        job.setReducerClass(PersonItemReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        return job;
    }

}
