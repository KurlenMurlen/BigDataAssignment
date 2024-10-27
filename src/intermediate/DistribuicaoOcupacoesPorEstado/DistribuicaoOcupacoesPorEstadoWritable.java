package intermediate.DistribuicaoOcupacoesPorEstado;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistribuicaoOcupacoesPorEstadoWritable implements WritableComparable<DistribuicaoOcupacoesPorEstadoWritable> {
    private String estado;
    private String ocupacao;

    public DistribuicaoOcupacoesPorEstadoWritable() {}

    public DistribuicaoOcupacoesPorEstadoWritable(String estado, String ocupacao) {
        this.estado = estado;
        this.ocupacao = ocupacao;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public String getOcupacao() {
        return ocupacao;
    }

    public void setOcupacao(String ocupacao) {
        this.ocupacao = ocupacao;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(estado);
        out.writeUTF(ocupacao);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        estado = in.readUTF();
        ocupacao = in.readUTF();
    }

    @Override
    public int compareTo(DistribuicaoOcupacoesPorEstadoWritable other) {
        int cmp = estado.compareTo(other.estado);
        if (cmp != 0) {
            return cmp;
        }
        return ocupacao.compareTo(other.ocupacao);
    }

    @Override
    public String toString() {
        return estado + " - " + ocupacao;
    }
}

