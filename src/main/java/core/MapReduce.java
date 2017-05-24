package core;

import conversion.ParseCSV;
import conversion.SerializeAvro;

/**
 * Created by marco on 24/05/17.
 */
public class MapReduce {

    public static void main(String[] args) {

        ParseCSV.parse("data/movies.csv");
        SerializeAvro.Serialize(ParseCSV.getMovies());
    }
}
