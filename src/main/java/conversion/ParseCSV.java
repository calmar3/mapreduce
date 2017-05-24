package conversion;

import model.Movie;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.UnknownFormatConversionException;

/**
 * Created by marco on 24/05/17.
 */
public class ParseCSV {

    private static List<Movie> movies = null;

    public static List<Movie> getMovies(){
        return movies;
    }

    public static void parse(String path){
        if (movies == null){
            movies = new ArrayList<Movie>();
        }
        try {
            Reader in = new FileReader(path);
            Iterable<CSVRecord> records = null;
            records = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records) {
                Movie temp = new Movie(record.get("movieId"),record.get("title"),record.get("genres"));
                movies.add(temp);
            }
        }
        catch (UnknownFormatConversionException e) {
            e.printStackTrace();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }


    }
}
