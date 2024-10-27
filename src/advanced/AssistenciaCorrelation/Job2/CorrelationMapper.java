package advanced.AssistenciaCorrelation.Job2;

import advanced.AssistenciaCorrelation.AssistenciaMedicaValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CorrelationMapper extends Mapper<LongWritable, Text, Text, AssistenciaMedicaValue> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        String municipioAnoEstCiv = fields[0] + "\t" + fields[1] + "\t" + fields[2];
        int assistencias = Integer.parseInt(fields[3]);
        int total = Integer.parseInt(fields[4]);

        context.write(new Text(municipioAnoEstCiv), new AssistenciaMedicaValue(assistencias, total));
    }
}
