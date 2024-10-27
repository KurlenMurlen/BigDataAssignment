package advanced.AssistenciaCorrelation;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MunicipioAnoEstCivKey implements WritableComparable<MunicipioAnoEstCivKey> {
    private String municipio;
    private int ano;
    private String estadoCivil;

    // Construtores
    public MunicipioAnoEstCivKey() { }

    public MunicipioAnoEstCivKey(String municipio, int ano, String estadoCivil) {
        this.municipio = municipio;
        this.ano = ano;
        this.estadoCivil = estadoCivil;
    }

    // Getters e Setters

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, municipio);
        out.writeInt(ano);
        WritableUtils.writeString(out, estadoCivil);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        municipio = WritableUtils.readString(in);
        ano = in.readInt();
        estadoCivil = WritableUtils.readString(in);
    }

    @Override
    public int compareTo(MunicipioAnoEstCivKey o) {
        int cmp = municipio.compareTo(o.municipio);
        if (cmp != 0) return cmp;
        cmp = Integer.compare(ano, o.ano);
        if (cmp != 0) return cmp;
        return estadoCivil.compareTo(o.estadoCivil);
    }

    @Override
    public String toString() {
        return municipio + "\t" + ano + "\t" + estadoCivil;
    }
}
