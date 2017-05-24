package conversion;

import model.Movie;
import model.Rating;
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
    private static List<Rating> ratings = null;

    public static List<Movie> getMovies(){
        return movies;
    }

    public static List<Rating> getRatings(){
        return ratings;
    }

    public static void parseMovies(String path){
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
            System.out.println(movies.size());
        }
        catch (UnknownFormatConversionException e) {
            e.printStackTrace();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void parseRatings(String path){
        if (ratings == null){
            ratings = new ArrayList<Rating>();
        }
        try {
            Reader in = new FileReader(path);
            Iterable<CSVRecord> records = null;
            records = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in);
            System.out.println("loop");
            for (CSVRecord record : records) {
                Rating temp = new Rating(record.get("userId"),record.get("movieId"),record.get("rating"),record.get("timestamp"));
                ratings.add(temp);
                System.gc();
            }
            System.out.println(ratings.size());
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
