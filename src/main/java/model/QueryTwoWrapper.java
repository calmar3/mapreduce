package model;

/**
 * Created by marco on 01/06/17.
 */
public class QueryTwoWrapper {

    private String[] genres;
    private Float rating;

    public QueryTwoWrapper(){
        this.genres = null;
        this.rating = null;

    }

    public String[] getGenres() {
        return genres;
    }

    public void setGenres(String[] genres) {
        this.genres = genres;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }


}
