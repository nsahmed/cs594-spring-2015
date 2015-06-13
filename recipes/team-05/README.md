
** Problem ** : How we can pull the flight delay data.

** Solution **: We can use the enigma API to pull the flight delays data.


package bigdata;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class FlightDelaysData {

	static String enigma_api = "6336800936fe4cd1fd565f4865f4e792";
	static String url = "https://api.enigma.io/v2/data/" + enigma_api;
	static String url2 = url
			+ "/us.gov.dot.rita.trans-stats.on-time-performance.2012?page=";
/* The following attributes were considered */
	static String[] flightAttributes = { "flightdate", "flightnum",
			"airlineid", "carrier", "originairportid", "origincityname",
			"originstatename", "destairportid", "destcityname", "deststate",
			"deptime", "arrtime", "lateaircraftdelay", "arrdelay",
			"divarrdelay", "depdelay", "weatherdelay", "securitydelay",
			"depdelayminutes", "nasdelay", "carrierdelay","origin","dest","dayofweek","dayofmonth","month","year","distance"};
	static JSONArray resultJsonArray = new JSONArray();
	static String path = "C:/Users//git/bigdata/resource/result.json";
	
	

	public static void main(String[] args) throws Exception {

		
		for(int i=1; i < 3; i++){
		URL url = new URL(url2+i);
		URLConnection urlConnection = url.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(
				urlConnection.getInputStream()));

		StringBuffer data = new StringBuffer();

		String inputLine;
		while ((inputLine = in.readLine()) != null) {
			data.append(inputLine);
			System.out.println(inputLine);
		}
		in.close();

		JSONParser jsonParser = new JSONParser();

		Object object = jsonParser.parse(data.toString());

		JSONObject json = (JSONObject) object;

		JSONArray jsonArray = (JSONArray) json.get("result");
		
		addJosnResultIntoArray(jsonArray);

		}
		writeJsonToFile(resultJsonArray,path);
		System.out.println(" === Done ===" + resultJsonArray.size());

	}

	
	/**
	 * 
	 * @param jsonArray
	 */
	public static void addJosnResultIntoArray(JSONArray jsonArray) {

		for (Object object : jsonArray) {
			JSONObject jsonObject = (JSONObject) object;

			JSONObject newJosnObject = new JSONObject();
			for (String att : flightAttributes) {
				newJosnObject.put(att, jsonObject.get(att));
			}

			resultJsonArray.add(newJosnObject);

		}

	}

	/**
	 * 
	 * @param jsonString
	 * @return
	 */
	public static String toPrettyFormat(String jsonString) {
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonString).getAsJsonObject();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String prettyJson = gson.toJson(json);
		return prettyJson;
	}
	
	
	/**
	 * 
	 * @param jsonObject
	 * @param filePath
	 * @throws IOException
	 */
	public static void writeJsonToFile(JSONArray jsonObject, String filePath)
			throws Exception {

		FileWriter file = new FileWriter(filePath, true);
		file.write(jsonObject.toJSONString());
		file.flush();
		file.close();

	}

}
** Problem **: Pull Weather History Data based on date and Aiport location.

** Solution **: Use UnderGroundWeather API to pull the Historical weather. 



package bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class WeatherData {

	public static final String API_KEY = "86a9b3e36957b844";

	public static String WEATHER_JSON_FILE = "C:/Users//git/bigdata/resource/weather_file.json";

	public static String path = "C:/Users//git/bigdata/resource/weather.json";

	public static String RESULT_JSON = "C:/Users//git/bigdata/resource/result.json";

	public static String HISTORY = "history";

	public static String DAILY_SUMMRY = "dailysummary";

	public static String MIN_VISIBILITY = "minvisi";

	public static String MAX_VISIBILITY = "maxvisi";

	public static String MAX_WIND_SPEED = "maxwspdi";

	public static String MAX_WIND_SPEED_MEAN = "maxwspdm";

	public static String OBSERVATION = "observations";

	public static String CONDITION = "conds";

	public static String ORIGIN_CITY = "origin";

	public static String DEST_CITY = "dest";

	public static String FLIGHT_DATE = "flightdate";

	public static String WEATHER_DETALY = "weatherdelay";

	// history -> dailysummary -> [0].minvisi
	// history -> dailysummary -> [0].maxvisi
	//
	// history -> dailysummary -> [0].maxwspdm
	// history -> dailysummary -> [0].maxwspdi

	public static void main(String[] args) throws Exception {

		// Read Flight Result files.
		JSONArray jsonArray = readJsonFileitoArray(RESULT_JSON);

		createWeatherURL(jsonArray);

		writeJsonToFile(jsonArray, WEATHER_JSON_FILE);

		System.out.println(" ******************************* ");
		
	}

	public static void createWeatherURL(JSONArray jsonArray) throws Exception {

		int i = 0;
		for (Object object : jsonArray) {
			JSONObject jsonObject = (JSONObject) object;

			if (isWeatherExist(jsonObject)) {
				url(jsonObject);
				i++;
			}

		}

		System.out.println("Total : " + i);
	}

	/**
	 * 
	 * @param jsonObject
	 * @return
	 */
	public static boolean isWeatherExist(JSONObject jsonObject) {

		String weatherDeal = (String) jsonObject.get(WEATHER_DETALY);

		if (weatherDeal != null) {
			Double value = new Double(weatherDeal);
			if (value > 0) {
				return true;
			}
		}

		return false;
	}

	/**
	 * 
	 * @param jsonObject
	 * @throws Exception
	 */
	public static void url(JSONObject jsonObject) throws Exception {
		String origin = (String) jsonObject.get(ORIGIN_CITY);
		String destination = (String) jsonObject.get(DEST_CITY);
		String flightDate = ISODateToString((String) jsonObject
				.get(FLIGHT_DATE));

		String originURL = "http://api.wunderground.com/api/86a9b3e36957b844/history_"
				+ flightDate + "/q/" + origin + ".json";
		String destURL = "http://api.wunderground.com/api/86a9b3e36957b844/history_"
				+ flightDate + "/q/" + destination + ".json";

		System.out.println(originURL);
		System.out.println(destURL);

		String UUIDString = UUID.randomUUID().toString();

		// Get Json from Orign URL
		String originJSON = getWeatherFromURL(originURL);

		JSONObject originWeatherJson = readJsonFile(originJSON);

		writeJsonToFile(originJSON, UUIDString + "_origin");

		// Get Json from Dest URL
		String destJSON = getWeatherFromURL(destURL);

		JSONObject destWeatherJson = readJsonFile(originJSON);

		writeJsonToFile(destJSON, UUIDString + "_dest");

		getWeatherAPI(jsonObject, "origin_weather", originWeatherJson,
				UUIDString);

		getWeatherAPI(jsonObject, "dest_weather", destWeatherJson, UUIDString);

	}

	public static void getWeatherAPI(JSONObject jsonObject1, String weatherFor,
			JSONObject weatherJson, String UUID) throws Exception {

		// weatherJson = readJsonFile(path);

		JSONObject weatherjsonObject = new JSONObject();

		weatherjsonObject.put(MIN_VISIBILITY,
				parseWeatherJSON(weatherJson, MIN_VISIBILITY));

		weatherjsonObject.put(MAX_VISIBILITY,
				parseWeatherJSON(weatherJson, MAX_VISIBILITY));

		weatherjsonObject.put(MAX_WIND_SPEED,
				parseWeatherJSON(weatherJson, MAX_WIND_SPEED));

		weatherjsonObject.put(MAX_WIND_SPEED_MEAN,
				parseWeatherJSON(weatherJson, MAX_WIND_SPEED_MEAN));

		weatherjsonObject.put(CONDITION, weatherCond(weatherJson));

		jsonObject1.put(weatherFor, weatherjsonObject);

		jsonObject1.put("UUID", UUID);

	}

	/**
	 * 
	 * @param jSONObject
	 * @param key
	 * @return
	 */
	public static String parseWeatherJSON(JSONObject jSONObject, String key) {

		JSONObject jSONObjectWeather = (JSONObject) jSONObject.get(HISTORY);

		if (jSONObjectWeather!=null && jSONObjectWeather.get(DAILY_SUMMRY) != null) {
			JSONArray jSONObjectDailySummry = (JSONArray) jSONObjectWeather
					.get(DAILY_SUMMRY);

			if (jSONObjectDailySummry != null
					&& jSONObjectDailySummry.size() > 0) {

				JSONObject dailyWeather = (JSONObject) jSONObjectDailySummry
						.get(0);
				return (String) dailyWeather.get(key);

			}
		}

		return "0.0";

	}

	/**
	 * 
	 * @param jSONObject
	 */
	public static JSONArray weatherCond(JSONObject jSONObject) {

		JSONObject jSONObjectWeather = (JSONObject) jSONObject.get(HISTORY);

		if (jSONObjectWeather!= null && jSONObjectWeather.get(OBSERVATION) != null) {
			JSONArray observationSummary = (JSONArray) jSONObjectWeather
					.get(OBSERVATION);

			JSONArray jsonArray = new JSONArray();

			if (observationSummary != null && observationSummary.size() > 0) {

				for (Object object : observationSummary) {
					JSONObject jsonObject = (JSONObject) object;
					System.out.println(" Condition : "
							+ jsonObject.get(CONDITION));

					jsonArray.add(jsonObject.get(CONDITION));
				}
			}
			return jsonArray;
		}

		return null;
	}

	public static String getWeatherFromURL(String url) throws Exception {

		// http://api.wunderground.com/api/86a9b3e36957b844/history_20101018/q/SFO.json

		// String url =
		// "http://api.wunderground.com/api/86a9b3e36957b844/history_20101018/q/SFO.json";

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// optional default is GET
		con.setRequestMethod("GET");

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
		System.out.println(response.toString());

		Thread.sleep(7000);

		return response.toString();
	}

	/**
	 * 
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public static JSONObject readJsonFile(String fileName) throws Exception {
		JSONParser parser = new JSONParser();
		// Object object = parser.parse(new FileReader(fileName));

		Object object = parser.parse(fileName);
		JSONObject jSONObject = (JSONObject) object;
		return jSONObject;
	}

	/**
	 * 
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public static JSONArray readJsonFileitoArray(String fileName)
			throws Exception {
		JSONParser parser = new JSONParser();
		Object object = parser.parse(new FileReader(fileName));
		JSONArray jsonArray = (JSONArray) object;
		return jsonArray;
	}

	/**
	 * 
	 * @param date
	 * @return
	 * @throws Exception
	 */
	public static String ISODateToString(String date) throws Exception {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		Date d = format.parse(date);
		SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
		return format1.format(d);
	}

	/**
	 * 
	 * @param jsonObject
	 * @param filePath
	 * @throws IOException
	 */
	public static void writeJsonToFile(JSONArray jsonObject, String filePath)
			throws Exception {

		FileWriter file = new FileWriter(filePath, true);
		file.write(jsonObject.toJSONString());
		file.flush();
		file.close();

	}

	/**
	 * 
	 * @param jsonObject
	 * @param filePath
	 * @throws Exception
	 */
	public static void writeJsonToFile(String jsonString, String fileName)
			throws Exception {

		String fileP = "C:/Users//git/bigdata/resource/" + fileName
				+ ".json";

		FileWriter file = new FileWriter(fileP, true);
		file.write(jsonString);
		file.flush();
		file.close();

	}

}

** Problem **: How to persist json file into mongo DB.

** Solution **: Use the Mongo DB build funtionlity to save json file.


package bigdata;

import java.io.FileReader;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class DataStorage {
	
	
	public static final String DB_NAME = "flights";
	public static final String TABLE_NAME = "flight";

	
	public static void main(String[] args) throws Exception {
		
		MongoClient mongoClient = new MongoClient( "localhost" );
		
		DB db = mongoClient.getDB(DB_NAME);
		
		DBCollection  collection = db.getCollection(TABLE_NAME);
		
					
		insert(readJsonFile(path),collection);
		
		DBCursor cursorDoc = collection.find();
		while (cursorDoc.hasNext()) {
			System.out.println(cursorDoc.next());
		}

		System.out.println("Done");
			
	}
	
	/**
	 * 
	 * @param jsonString
	 * @param collection
	 */
	public static void insert(String jsonString, DBCollection collection){
		
		 BasicDBList data = (BasicDBList) JSON.parse(jsonString);
		    for(int i=0; i < data.size(); i++){
		    	collection.insert((DBObject) data.get(i));
		    }
		
	}
	
	/**
	 * 
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public static String readJsonFile(String fileName) throws Exception {
		JSONParser parser = new JSONParser();
		Object object = parser.parse(new FileReader(fileName));
		JSONArray jsonArray = (JSONArray) object;	
		return jsonArray.toJSONString();
	}
	public static String path = "C:/Users/CS580/git/bigdata/resource/result.json";
}

** Problem **: How to calculate standard deviation on given data.

** Solution **: We can use the following code to get find the standard deviation.



package bigdata;

import java.util.Arrays;

public class Statistics {
	double[] data;
	double size;

	public Statistics(double[] data) {
		this.data = data;
		size = data.length;
	}


/*This method is to get the mean of data */

	double getMean() {
		double sum = 0.0;
		for (double a : data)
			sum += a;
		return sum / size;
	}

/*This method is to get the varience of data */

	double getVariance() {
		double mean = getMean();
		double temp = 0;
		for (double a : data)
			temp += (mean - a) * (mean - a);
		return temp / size;
	}


/*This method is to get the standard deviation of data */

	double getStdDev() {
		return Math.sqrt(getVariance());
	}

/*This method is to get the median of data */

	public double median() {
		double[] b = new double[data.length];
		System.arraycopy(data, 0, b, 0, b.length);
		Arrays.sort(b);

		if (data.length % 2 == 0) {
			return (b[(b.length / 2) - 1] + b[b.length / 2]) / 2.0;
		} else {
			return b[b.length / 2];
		}
	}
}

## Reference
Source: 1. http://api.wunderground.com/api/86a9b3e36957b844/history_
        2. https://api.enigma.io/v2/data/



