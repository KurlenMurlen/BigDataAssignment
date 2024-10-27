package advanced.AssistenciaCorrelation.Job2;

import advanced.AssistenciaCorrelation.AssistenciaMedicaValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CorrelationReducer extends Reducer<Text, AssistenciaMedicaValue, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<AssistenciaMedicaValue> values, Context context) throws IOException, InterruptedException {
        int totalAssistencias = 0;
        int totalOcorrencias = 0;

        for (AssistenciaMedicaValue value : values) {
            totalAssistencias += value.getAssistenciaPresente().get();
            totalOcorrencias += value.getTotal().get();
        }

        // Calcula o percentual
        double percentual = ((double) totalAssistencias / totalOcorrencias) * 100;

        // Emite a chave e o percentual
        context.write(key, new Text(String.format("%.2f%%", percentual)));
    }
}
