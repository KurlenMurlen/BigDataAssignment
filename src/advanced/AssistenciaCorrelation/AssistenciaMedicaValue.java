package advanced.AssistenciaCorrelation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AssistenciaMedicaValue implements Writable {
    private IntWritable assistenciaPresente;  // 1 se há assistência, 0 caso contrário
    private IntWritable total;  // Total de ocorrências

    // Construtor padrão
    public AssistenciaMedicaValue() {
        this.assistenciaPresente = new IntWritable();
        this.total = new IntWritable();
    }

    // Construtor
    public AssistenciaMedicaValue(int assistencia, int total) {
        this.assistenciaPresente = new IntWritable(assistencia);
        this.total = new IntWritable(total);
    }

    // Getters e setters
    public IntWritable getAssistenciaPresente() {
        return assistenciaPresente;
    }

    public IntWritable getTotal() {
        return total;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        assistenciaPresente.write(out);
        total.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        assistenciaPresente.readFields(in);
        total.readFields(in);
    }

    @Override
    public String toString() {
        return assistenciaPresente + "\t" + total;
    }
}
