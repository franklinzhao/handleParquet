package parquetReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import org.apache.parquet.example.data.simple.SimpleGroup;

public class App {
	public static void main(String[] args) throws URISyntaxException {
		System.out.println("Hello World!");
//      // Replace this with the path to your local temporary folder
		String fileName = "housePrice.parquet"; // Replace with actual filename
		String file1="Flights.parquet";
		String file2="Iris.parquet";
		String file3="MTcars.parquet";
		String file4="Titanic.parquet";
		String file5="Weather.parquet";
		
		String filePath = "test.txt";
		String pfilePath=getFilePath(file4);
//		String pfilePath2=getFilePath(args);
//		String inputFile ="/Users/franklinzhao/Downloads/housePrice.parquet";

//		readFileFromResources(filePath);

//		Path filePath = Paths.get(tempFolderPath, fileName);
		ParquetDataWithSchema parquet = null;
		try {
			parquet = ParquetReaderUtils.getParquetData(pfilePath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("parquet: "+parquet.getSchema().toString());
		SimpleGroup simpleGroup = parquet.getData().get(9);
//		String storedString = simpleGroup.getString("price", 0);
		
//		System.out.println("stored field: "+simpleGroup.getString("furnishingstatus", 0));
		System.out.println("column and row: "+simpleGroup.getValueToString(4, 0));
		
		final HashMap<Integer, Object> dataMap = new HashMap<>();
		List<SimpleGroup> simpleGroups = parquet.getData();
		for(int i=0;i<simpleGroups.size();i++) {
			dataMap.put(i, simpleGroups.get(i));
		}
		 System.out.println("field type: "+parquet.getSchema().get(0).getOriginalType());
		 System.out.println("field type: "+parquet.getSchema().get(0).asPrimitiveType());
		
		dataMap.keySet().stream().forEach(new Consumer<Integer>() {
			@Override
			public void accept(Integer k) {
				System.out.println("key: "+k+" value: "+dataMap.get(k).toString());
			}
		});
	}

	private static void readFileFromResources(String fileName) {
		// Get the class loader object
        ClassLoader classLoader = App.class.getClassLoader();

        // Get the resource as an InputStream
        InputStream inputStream = classLoader.getResourceAsStream(fileName);

        // Check if the resource exists
        if (inputStream == null) {
            System.err.println("Error: File not found: " + fileName);
            return;
        }

        // Read the file content line by line
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public static String getFilePath(String fileName) {
		ClassLoader classLoader = App.class.getClassLoader();
        URL resourceUrl = classLoader.getResource(fileName);

        if (resourceUrl == null) {
            System.err.println("Error: File not found: " + fileName);
            return null;
        }

        String filePath = resourceUrl.getPath();  // Get path from URL
        System.out.println("File Path: " + filePath);
        
        return filePath;
	}
	
	
	// get a file from the resources folder
    // works everywhere, IDEA, unit test and JAR file.
    public InputStream getFileFromResourceAsStream(String fileName) {

        // The class loader that loaded the class
        ClassLoader classLoader = null;
		try {
			classLoader = getClass().getClassLoader();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        InputStream inputStream = classLoader.getResourceAsStream(fileName);

        // the stream holding the file content
        if (inputStream == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            return inputStream;
        }

    }

    
    // print input stream
    public static void printInputStream(InputStream is) {

        try (InputStreamReader streamReader =
                    new InputStreamReader(is, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    
}
