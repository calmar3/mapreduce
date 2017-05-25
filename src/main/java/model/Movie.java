package model;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

/**
 * Created by marco on 24/05/17.
 */
public class Movie {

    private String movieId;
    private String title;
    private String genres;

    public Movie(){

    }
    public static Schema getSchema() {
        return ReflectData.get().getSchema(Movie.class);
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }
}
