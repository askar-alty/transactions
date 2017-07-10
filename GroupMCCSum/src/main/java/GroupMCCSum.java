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
    public static class GroupMCCSumMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");
            String mccCode = fields[3];
            String sumTransaction = fields[7];

            if (!sumTransaction.equals("trx_rur_amt")
                    && !sumTransaction.equals("?")
                    && !mccCode.equals("?")
                    && !mccCode.equals("9999")
                    && !mccCode.equals("9998")
                    && !mccCode.equals("9997")
                    && !mccCode.equals("9996")
                    && !mccCode.equals("5495")) {
                context.write(new Text(mccCode), new Text(sumTransaction + ";1"));
            }
        }
    }

    // Combiner
    public static class GroupMCCSumCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            for (Text value : values) {
                String[] fields = value.toString().split(";");

                sum += Double.parseDouble(fields[0].replace(",", "."));
                count += Integer.parseInt(fields[1]);
            }
            context.write(key, new Text(String.valueOf(sum) + "," + String.valueOf(count)));
        }
    }


    // Reducer
    public static class GroupMCCSumReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            for (Text value : values) {
                String[] fields = value.toString().split(",");
                sum += Double.parseDouble(fields[0]);
                count += Integer.parseInt(fields[1]);
            }
            context.write(key, new Text(String.valueOf(sum) + "\t" + String.valueOf(count)));
        }
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConf(); // конфигурации воркера

        Job job = Job.getInstance(configuration, "Group by mcc codes"); /// создаем воркера
        job.setJarByClass(this.getClass());

        Path in_put = new Path(args[0]); // путь к входным файлам в hdfs
        Path out_put = new Path(args[1]); // путь к выходному файлу в hdfs

        TextInputFormat.addInputPath(job, in_put);
        TextOutputFormat.setOutputPath(job, out_put);

        job.setMapperClass(GroupMCCSumMapper.class); // mapper class
        job.setCombinerClass(GroupMCCSumCombiner.class); // combiner class
        job.setReducerClass(GroupMCCSumReducer.class); // reducer class

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class); // тип ключа на выходе маппера
        job.setOutputValueClass(Text.class); // тип значения на выходе маппера

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new GroupMCCSum(), args);
        System.exit(exitCode);
    }
}
