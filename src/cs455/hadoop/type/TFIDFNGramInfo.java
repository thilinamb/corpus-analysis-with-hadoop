package cs455.hadoop.type;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/15/14
 */
public class TFIDFNGramInfo implements Writable {

    private Text documentId;
    private DoubleWritable tfValue;

    public TFIDFNGramInfo() {
        this.documentId = new Text();
        this.tfValue = new DoubleWritable();
    }

    public TFIDFNGramInfo(Text documentId, DoubleWritable tfValue) {
        this.documentId = documentId;
        this.tfValue = tfValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        documentId.write(dataOutput);
        tfValue.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        documentId.readFields(dataInput);
        tfValue.readFields(dataInput);
    }

    public Text getDocumentId() {
        return documentId;
    }

    public void setDocumentId(Text documentId) {
        this.documentId = documentId;
    }

    public DoubleWritable getTfValue() {
        return tfValue;
    }

    public void setTfValue(DoubleWritable tfValue) {
        this.tfValue = tfValue;
    }
}
