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

    public Movie(String movieId, String title, String genres){
        this.movieId = movieId;
        this.title = title;
        this.genres = genres;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "movieId=" + movieId +
                ", title='" + title + '\'' +
                ", genres=" + genres +
                '}';
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

    public Schema getSchema() {
        return ReflectData.get().getSchema(Movie.class);
    }
}
