package model;

/**
 * Created by marco on 03/06/17.
 */
public class QueryThreeWrapper implements Comparable<QueryThreeWrapper>{

    private Float avg;

    private String title;

    private Integer ratingsNumber;

    private Float rating;

    public QueryThreeWrapper(){
        this.avg = null;
        this.title = null;
        this.ratingsNumber = null;
        this.rating = null;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Float getAvg() {
        return avg;
    }

    public void setAvg(Float avg) {
        this.avg = avg;
    }

    public Integer getRatingsNumber() {
        return ratingsNumber;
    }

    public void setRatingsNumber(Integer sum) {
        this.ratingsNumber = sum;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }

    public int compareTo(QueryThreeWrapper o) {
        if (this.avg > o.getAvg())
            return -1;
        else if (this.avg < o.getAvg())
            return 1;
        else{
            if (this.ratingsNumber > o.getRatingsNumber())
                return -1;
            else if (this.ratingsNumber < o.getRatingsNumber())
                return 1;
            return (this.getTitle().compareTo(o.getTitle()));
        }
    }
}
