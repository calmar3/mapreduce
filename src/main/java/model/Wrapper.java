package model;

/**
 * Created by marco on 30/05/17.
 */
public class Wrapper{

    private String title;
    private Float rating;

    public Wrapper(){
        this.title = null;
        this.rating = null;
    }


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }
}
