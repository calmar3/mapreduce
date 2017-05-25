package conversion;

import model.Movie;
import model.Rating;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.UnknownFormatConversionException;

/**
 * Created by marco on 24/05/17.
 */
public class CsvToAvro {


    public static void parseMovies(String input,String output){
        try {

            Reader in = new FileReader(input);
            Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in);
            DatumWriter<Movie> movieDatumWriter = new ReflectDatumWriter<Movie>(Movie.class);
            DataFileWriter<Movie> dataFileWriter = new DataFileWriter<Movie>(movieDatumWriter);
            Movie temp = new Movie();
            dataFileWriter.create(temp.getSchema(), new File(output));
            System.out.println("movies loop");
            for (CSVRecord record : records) {
                temp.setMovieId(record.get("movieId"));
                temp.setTitle(record.get("title"));
                temp.setGenres(record.get("genres"));
                dataFileWriter.append(temp);
            }
            dataFileWriter.close();
            System.out.println("movies writed");
        }
        catch (UnknownFormatConversionException e) {
            e.printStackTrace();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void parseRatings(String path,String output){
        try {
            Reader in = new FileReader(path);
            Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in);
            DatumWriter<Rating> ratingDatumWriter = new ReflectDatumWriter<Rating>(Rating.class);
            DataFileWriter<Rating> dataFileWriter = new DataFileWriter<Rating>(ratingDatumWriter);
            Rating temp = new Rating();
            dataFileWriter.create(temp.getSchema(), new File(output));
            System.out.println("ratings loop");
            for (CSVRecord record : records) {
                temp.setUserId(record.get("userId"));
                temp.setMovieId(record.get("movieId"));
                temp.setRating(record.get("rating"));
                temp.setTimestamp(record.get("timestamp"));
                dataFileWriter.append(temp);
            }
            dataFileWriter.close();
            System.out.println("ratings writed");
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
