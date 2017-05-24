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



    public Schema getSchema() {
        return ReflectData.get().getSchema(Rating.class);
    }
}
