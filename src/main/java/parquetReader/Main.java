package parquetReader;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {


		public static void main(String[] args) throws ParseException {
//			date format (yyyy = year, ddd=day) as a 'Julian Date' - this is the common term for such a date in mainframe and other circles.
	        String inputDate = "2017365";

	        // Define input and output date formats
	        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyddd");
	        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");

	        // Parse the input date string
	        Date date = inputFormat.parse(inputDate);

	        // Format the date to ISO format
	        String isoDate = outputFormat.format(date);

	        System.out.println("Original date: " + inputDate);
	        System.out.println("ISO formatted date: " + isoDate);


	}

}
