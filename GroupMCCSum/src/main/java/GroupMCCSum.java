import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Askar Shabykov
 * @since 07.07.17
 */

public class GroupMCCSum extends Configured implements Tool {


    // Mapper
    public static class GroupMCCSumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");
            String mccCode = fields[3];

            if (!mccCode.equals("?") && !mccCode.equals("9999") && !mccCode.equals("9998") && !mccCode.equals("9997") && !mccCode.equals("9996") && !mccCode.equals("5495")) {
                context.write(new Text(mccCode), one);
            }
        }
    }

    // Combiner
    public static class GroupMCCSumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            for (Text value : values) {
                String[] fields = value.toString().split(",");
                sum += Double.parseDouble(fields[0]);
                count += Integer.parseInt(fields[1]);
            }
            context.write(key, new Text(String.valueOf(sum) + "," + String.valueOf(count)));
        }
    }


    // Reducer
    public static class GroupMCCSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }


    public int run(String[] args) throws Exception {
        return 0;
    }
}
