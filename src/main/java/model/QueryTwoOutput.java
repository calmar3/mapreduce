package model;

/**
 * Created by marco on 01/06/17.
 */
public class QueryTwoOutput {


        private Double stdev;
        private Float avg;

        public QueryTwoOutput(){
            this.stdev = null;
            this.avg = null;

        }


    public Double getStdev() {
        return stdev;
    }

    public void setStdev(Double stdev) {
        this.stdev = stdev;
    }

    public Float getAvg() {
        return avg;
    }

    public void setAvg(Float avg) {
        this.avg = avg;
    }
}
