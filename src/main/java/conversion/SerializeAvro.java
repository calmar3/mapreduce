package conversion;

import model.Movie;
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

    public static void Serialize(List<Movie> movies){
        DatumWriter<Movie> movieDatumWriter = new ReflectDatumWriter<Movie>(Movie.class);
        DataFileWriter<Movie> dataFileWriter = new DataFileWriter<Movie>(movieDatumWriter);
        try {
            dataFileWriter.create(movies.get(0).getSchema(), new File("data/movies.avro"));
            for (Movie mv : movies){
                dataFileWriter.append(mv);
            }
            dataFileWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
