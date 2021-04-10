package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ForestFireWritable implements WritableComparable<ForestFireWritable> {
    private double temp;
    private double vento;

    // Construtro vazio
    public ForestFireWritable() {
    }

    // Construtro 'não-vazio'
    public ForestFireWritable(double temp, double vento) {
        this.temp = temp;
        this.vento = vento;
    }

    // Getters
    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    // Setters
    public double getVento() {
        return vento;
    }

    public void setVento(double vento) {
        this.vento = vento;
    }

    // Compare To
    @Override
    public int compareTo(ForestFireWritable o) {
        return 0;
    }
    // Write Fields
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // manter a mesma ordem no write e no read
        dataOutput.writeDouble(temp);
        dataOutput.writeDouble(vento);
    }
    // Read Fields
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // manter a mesma ordem no write e no read
        temp = dataInput.readDouble();
        vento = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "ForestFireWritable{" +
                "temp=" + temp +
                ", vento=" + vento +
                '}';
    }

    // equals
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForestFireWritable that = (ForestFireWritable) o;
        return Double.compare(that.temp, temp) == 0 && Double.compare(that.vento, vento) == 0;
    }

    // hashCode
    @Override
    public int hashCode() {
        return Objects.hash(temp, vento);
    }
}
