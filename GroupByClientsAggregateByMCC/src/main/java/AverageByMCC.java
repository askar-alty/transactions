
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class AverageByMCC extends Configured implements Tool {


    // Mapper
    public static class AverageByMCCMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");
            String client = fields[1];
            String mccCode = fields[3];

            if (!client.equals("?") && !client.equals("card_client_w4_id") && !mccCode.equals("?") && !mccCode.equals("9999") && !mccCode.equals("9998") && !mccCode.equals("9997") && !mccCode.equals("9996") && !mccCode.equals("5495")) {
                context.write(new Text(client), new Text(mccCode));
            }
        }
    }


    // Reducer
    public static class AverageByMCCReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder mccCodes = new StringBuilder();
            int i = 0;
            for (Text value : values) {
                if (i == 0) {
                    mccCodes.append(value.toString());
                } else {
                    mccCodes.append(",").append(value.toString());
                }
                i++;
            }
            context.write(key, new Text(String.valueOf(mccCodes)));
        }
    }


    public int run(String[] args) throws Exception {
        Configuration configuration = getConf(); // конфигурации воркера

        Job job = Job.getInstance(configuration, "Average By MCC"); /// создаем воркера
        job.setJarByClass(this.getClass());


        Path in_put = new Path(args[0]); // путь к входным файлам в hdfs
        Path out_put = new Path(args[1]); // путь к выходному файлу в hdfs


        TextInputFormat.addInputPath(job, in_put);
        TextOutputFormat.setOutputPath(job, out_put);


        job.setMapperClass(AverageByMCCMapper.class); // mapper class
        job.setReducerClass(AverageByMCCReducer.class); // reducer class

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class); // тип ключа на выходе маппера
        job.setOutputValueClass(Text.class); // тип значения на выходе маппера

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new AverageByMCC(), args);
        System.exit(exitCode);
    }
}
