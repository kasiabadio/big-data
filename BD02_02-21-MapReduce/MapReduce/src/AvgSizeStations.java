import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import java.io.IOException;

// jaka jest wielkość (wyrażana za pomocą średniej liczby stanowisk)
// stacji rowerowych oddawanych do użytku w poszczególnych latach?

public class AvgSizeStations extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AvgSizeStations(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "AvgSizeStations");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //TODO: set mapper and reducer class

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // klucz wejściowy, wartość wejściowa, klucz pośredni, wartość pośrednia
    public static class AvgSizeStationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text year = new Text();
        private final IntWritable size = new IntWritable();

        // creates key-value pair of {rok z install_date}-{install_dockcount}
        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) { // if it is the first row, which is a header
                    String line = lineText.toString();
                    int i = 0;
                    for (String word : line
                            .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                        if (i == 4) { // install_date
                            year.set(word.substring(word.lastIndexOf('/') + 1,
                                    word.lastIndexOf('/') + 5));
                        }
                        if (i == 5) { // install_dockcount
                            size.set(Integer.parseInt(word));
                        }
                        i++;
                    }
                    //TODO: write intermediate pair to the context

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // klucz pośredni, wartość pośrednia, klucz wyjściowy, wartość wyjściowa funkcji redukcyjnej
    public static class AvgSizeStationReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private final DoubleWritable resultValue = new DoubleWritable();
        Float average;
        Float count;
        int sum;

        // counts average value of {install_dockcount} for each key
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            average = 0f;
            count = 0f;
            sum = 0;

            Text resultKey = new Text("average station size in " + key + " was: ");

            for (IntWritable val : values) {
                sum += val.get();
                count += 1;
            }
            //TODO: set average variable properly

            resultValue.set(average);
            //TODO: write result pair to the context

        }
    }

    public static class AvgSizeStationCombiner extends Reducer<Text, SumCount, Text, SumCount> {

        private final SumCount sum = new SumCount(0.0d, 0);

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {

            sum.set(new DoubleWritable(0.0d), new IntWritable(0));

            for (SumCount val : values) {
                sum.addSumCount(val);
            }
            context.write(key, sum);
        }
    }
}