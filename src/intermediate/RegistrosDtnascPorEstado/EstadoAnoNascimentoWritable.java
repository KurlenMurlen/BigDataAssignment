package intermediate.RegistrosDtnascPorEstado;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EstadoAnoNascimentoWritable implements WritableComparable<EstadoAnoNascimentoWritable> {
    private String estado;
    private String anoNascimento;

    public EstadoAnoNascimentoWritable() {}

    public EstadoAnoNascimentoWritable(String estado, String anoNascimento) {
        this.estado = estado;
        this.anoNascimento = anoNascimento;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public String getAnoNascimento() {
        return anoNascimento;
    }

    public void setAnoNascimento(String anoNascimento) {
        this.anoNascimento = anoNascimento;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(estado);
        out.writeUTF(anoNascimento);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        estado = in.readUTF();
        anoNascimento = in.readUTF();
    }

    @Override
    public int compareTo(EstadoAnoNascimentoWritable other) {
        int cmp = estado.compareTo(other.estado);
        if (cmp != 0) {
            return cmp;
        }
        return anoNascimento.compareTo(other.anoNascimento);
    }

    @Override
    public String toString() {
        return estado + " - " + anoNascimento;
    }
}

