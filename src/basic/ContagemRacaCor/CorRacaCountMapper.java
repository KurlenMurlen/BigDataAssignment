package basic.ContagemRacaCor;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CorRacaCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] columns = line.split(",");
        if (columns.length > 7) {
            String corRaca = columns[7];
            context.write(new Text(corRaca), new IntWritable(1));
        }
    }
}
