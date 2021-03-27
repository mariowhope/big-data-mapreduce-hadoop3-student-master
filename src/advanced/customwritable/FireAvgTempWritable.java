package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

// Java bean -> classe com as seguintes caracteristicas:   atributos privados , gets e sets e contrutor padrao (vazio)
public class FireAvgTempWritable implements WritableComparable<FireAvgTempWritable> {
    private long ocorrencia;
    private double somaTemp;

    // Contrutor vazio
    public FireAvgTempWritable() {
    }

    // Contrutor 'não vazio'
    public FireAvgTempWritable(long ocorrencia, double somaTemp) {
        this.ocorrencia = ocorrencia;
        this.somaTemp = somaTemp;
    }

    // Getters
    public long getOcorrencia() {
        return ocorrencia;
    }

    public double getSomaTemp() {
        return somaTemp;
    }

    // Setters
    public void setOcorrencia(long ocorrencia) {
        this.ocorrencia = ocorrencia;
    }

    public void setSomaTemp(double somaTemp) {
        this.somaTemp = somaTemp;
    }

    // To String
    @Override
    public String toString() {
        return "FireAvgTempWritable{" +
                "ocorrencia=" + ocorrencia +
                ", somaTemp=" + somaTemp +
                '}';
    }
    // Métodos write/read :: Servem para serializar de forma customizada (necessário para transferência pela rede pelo contexto)
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(ocorrencia);
        dataOutput.writeDouble(somaTemp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ocorrencia = dataInput.readLong();
        somaTemp = dataInput.readDouble();
    }

    //
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    //
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    //
    @Override
    public int compareTo(FireAvgTempWritable o) {
        /*
         valor composto ::a função do compareTo nao tem mto sentido para valores compostos,
         porém para chaves compostas isso sera imporante para o map reduce saber como ira
         ordenar as chaves no processo de sort/shuffle.
         Caso ele não saiba como comparar as hashs ele irá se perder;
        */
        if(hashCode() < o.hashCode()){
            return -1;
        }else if(hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }

}
