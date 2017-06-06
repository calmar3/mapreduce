package model;

/**
 * Created by marco on 06/06/17.
 */
public class QueryThreeRankOutput implements Comparable<QueryThreeRankOutput>{

    private Float avg;

    private Integer number;

    private String title;

    public QueryThreeRankOutput(){
        this.avg = null;
        this.number = null;
        this.title = null;

    }

    @Override
    public String toString() {
        return "QueryThreeRankOutput{" +
                "avg=" + avg +
                ", ratingsNumber=" + number +
                ", title='" + title + '\'' +
                '}';
    }

    public Float getAvg() {
        return avg;
    }

    public void setAvg(Float avg) {
        this.avg = avg;
    }



    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer ratingsNumber) {
        this.number = ratingsNumber;
    }


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public int compareTo(QueryThreeRankOutput o) {
        if (this.avg > o.getAvg())
            return -1;
        else if (this.avg < o.getAvg())
            return 1;
        else{
            if (this.number > o.getNumber())
                return -1;
            else if (this.number < o.getNumber())
                return 1;
            return (this.getTitle().compareTo(o.getTitle()));
        }
    }
}
