import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class LogMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] parts = value.toString().split(" ");
            String ip = parts[0];
            context.write(new Text(ip), one);
        } catch (Exception e) {
            //
        }
    }

}
