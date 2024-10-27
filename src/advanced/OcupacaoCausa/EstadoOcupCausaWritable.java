package advanced.OcupacaoCausa;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EstadoOcupCausaWritable implements WritableComparable<EstadoOcupCausaWritable> {
    private String estado;
    private String ocupacao;
    private String causaBasica;

    // Construtor vazio
    public EstadoOcupCausaWritable() {}

    // Construtor completo
    public EstadoOcupCausaWritable(String estado, String ocupacao, String causaBasica) {
        this.estado = estado;
        this.ocupacao = ocupacao;
        this.causaBasica = causaBasica;
    }

    // Getters e Setters
    public String getEstado() { return estado; }
    public void setEstado(String estado) { this.estado = estado; }

    public String getOcupacao() { return ocupacao; }
    public void setOcupacao(String ocupacao) { this.ocupacao = ocupacao; }

    public String getCausaBasica() { return causaBasica; }
    public void setCausaBasica(String causaBasica) { this.causaBasica = causaBasica; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(estado);
        out.writeUTF(ocupacao);
        out.writeUTF(causaBasica);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        estado = in.readUTF();
        ocupacao = in.readUTF();
        causaBasica = in.readUTF();
    }

    @Override
    public int compareTo(EstadoOcupCausaWritable o) {
        int cmp = estado.compareTo(o.getEstado());
        if (cmp != 0) {
            return cmp;
        }
        cmp = ocupacao.compareTo(o.getOcupacao());
        if (cmp != 0) {
            return cmp;
        }
        return causaBasica.compareTo(o.getCausaBasica());
    }

    @Override
    public int hashCode() {
        return estado.hashCode() * 163 + ocupacao.hashCode() * 163 + causaBasica.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EstadoOcupCausaWritable that = (EstadoOcupCausaWritable) o;
        return estado.equals(that.estado) && ocupacao.equals(that.ocupacao) && causaBasica.equals(that.causaBasica);
    }

    @Override
    public String toString() {
        return estado + "\t" + ocupacao + "\t" + causaBasica;
    }
}
