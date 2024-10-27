package advanced.AssistenciaCorrelation.Job1;

import advanced.AssistenciaCorrelation.MunicipioAnoEstCivKey;
import org.apache.hadoop.mapreduce.Reducer;
import advanced.AssistenciaCorrelation.AssistenciaMedicaValue;

import java.io.IOException;

public class AssistenciaReducer extends Reducer<MunicipioAnoEstCivKey, AssistenciaMedicaValue, MunicipioAnoEstCivKey, AssistenciaMedicaValue> {

    @Override
    protected void reduce(MunicipioAnoEstCivKey key, Iterable<AssistenciaMedicaValue> values, Context context) throws IOException, InterruptedException {
        int totalAssistencias = 0;
        int totalOcorrencias = 0;

        // Soma o número de assistências e ocorrências
        for (AssistenciaMedicaValue value : values) {
            totalAssistencias += value.getAssistenciaPresente().get();
            totalOcorrencias += value.getTotal().get();
        }

        // Emite o resultado agregado
        context.write(key, new AssistenciaMedicaValue(totalAssistencias, totalOcorrencias));
    }
}
