package core;

import conversion.ParseCSV;
import conversion.SerializeAvro;

/**
 * Created by marco on 24/05/17.
 */
public class MapReduce {

    public static void main(String[] args) {

        //ParseCSV.parseMovies("data/movies.csv");
        ParseCSV.parseRatings("data/ratings.csv");
        //SerializeAvro.serializeMovies("data/movies.avro");
        SerializeAvro.serializeRatings("data/ratings.avro");
    }
}
