package model;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by marco on 30/05/17.
 */
public class Wrapper {

    private Text text;
    private FloatWritable floatWritable;

    public Wrapper(){
        this.text = null;
        this.floatWritable = null;
    }

    public Text getText() {
        return text;
    }

    public void setText(Text text) {
        this.text = text;
    }

    public FloatWritable getFloatWritable() {
        return floatWritable;
    }

    public void setFloatWritable(FloatWritable floatWritable) {
        this.floatWritable = floatWritable;
    }
}
