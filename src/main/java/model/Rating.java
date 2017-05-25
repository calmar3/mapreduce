package model;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

/**
 * Created by marco on 24/05/17.
 */
public class Rating {

    private String userId;
    private String movieId;
    private String rating;
    private String timestamp;

    public Rating(String userId, String movieId, String rating,String timestamp){
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public Rating(){

    }



    public Schema getSchema() {
        return ReflectData.get().getSchema(Rating.class);
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public String getRating() {
        return rating;
    }

    public void setRating(String rating) {
        this.rating = rating;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
