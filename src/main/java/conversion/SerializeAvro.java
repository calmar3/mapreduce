package conversion;

import model.Movie;
import model.Rating;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by marco on 24/05/17.
 */
public class SerializeAvro {

    public static void serializeMovies(String path){
        List<Movie> movies = ParseCSV.getMovies();
        DatumWriter<Movie> movieDatumWriter = new ReflectDatumWriter<Movie>(Movie.class);
        DataFileWriter<Movie> dataFileWriter = new DataFileWriter<Movie>(movieDatumWriter);
        try {
            dataFileWriter.create(movies.get(0).getSchema(), new File(path));
            for (Movie mv : movies){
                dataFileWriter.append(mv);
            }
            dataFileWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void serializeRatings(String path){
        List<Rating> ratings = ParseCSV.getRatings();
        DatumWriter<Rating> ratingDatumWriter = new ReflectDatumWriter<Rating>(Rating.class);
        DataFileWriter<Rating> dataFileWriter = new DataFileWriter<Rating>(ratingDatumWriter);
        try {
            dataFileWriter.create(ratings.get(0).getSchema(), new File(path));
            System.out.println("loop serialize");
            for (Rating rt : ratings){
                dataFileWriter.append(rt);
            }
            System.out.println("serialized");
            dataFileWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
