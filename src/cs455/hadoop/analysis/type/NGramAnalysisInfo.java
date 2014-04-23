package cs455.hadoop.analysis.type;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/22/14
 */
public class NGramAnalysisInfo implements Writable {

    private int period;
    private double tfValue;
    private double idfValue;
    private double tfIdfValue;

    public NGramAnalysisInfo(int period, double tfValue, double idfValue, double tfIdfValue) {
        this.period = period;
        this.tfValue = tfValue;
        this.idfValue = idfValue;
        this.tfIdfValue = tfIdfValue;
    }

    public NGramAnalysisInfo() {

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(period);
        out.writeDouble(tfValue);
        out.writeDouble(idfValue);
        out.writeDouble(tfIdfValue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        period = in.readInt();
        tfValue = in.readDouble();
        idfValue = in.readDouble();
        tfIdfValue = in.readDouble();
    }

    public double getTfValue() {
        return tfValue;
    }

    public void setTfValue(double tfValue) {
        this.tfValue = tfValue;
    }

    public double getIdfValue() {
        return idfValue;
    }

    public void setIdfValue(double idfValue) {
        this.idfValue = idfValue;
    }

    public double getTfIdfValue() {
        return tfIdfValue;
    }

    public void setTfIdfValue(double tfIdfValue) {
        this.tfIdfValue = tfIdfValue;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }
}
