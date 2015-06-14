
** Problem ** : How we can pull the flight delay data.

** Solution **: We can use the enigma API to pull the flight delays data.


	
# The faced a problem while importing data using Enigma API because the API only fetches 500 rows
			therefore, to get more number of rows we have used for loop as seen in the code block in 
			FlightDelaysData.java file.
			In the given code block, the enigma API is called 10 times (for example) and thus it is importing
			dataset of 5000 rows.
			Please refer to WeatherData.java to see the code. 


# The following attributes were considered out of 111 attributes
		     {"flightdate", "flightnum",
			"airlineid", "carrier", "originairportid", "origincityname",
			"originstatename", "destairportid", "destcityname", "deststate",
			"deptime", "arrtime", "lateaircraftdelay", "arrdelay",
			"divarrdelay", "depdelay", "weatherdelay", "securitydelay",
			"depdelayminutes", "nasdelay", "carrierdelay","origin","dest","dayofweek",
			"dayofmonth","month","year","distance"} 
	

** Problem **: How to increase the reliability of the raw data.

** Solution **: We have used the Enigma API to get our raw data. However, using only
                one data source may arise the question of reliability and dependability 
				of the data. We wanted to use UCI's Data Warehouse's weather database but
				it required lots of cleaning and preparation.
				Since we have taken mostly weather parameters and location as delay parameters,
				we wanted to support our data reliability by using UnderGroundWeather API 
				(wunderground) to pull the Historical weather. 
                Please refer to WeatherData.java to see the code. 
		
		
** Problem **: Pull Weather History Data based on date and Aiport location.

** Solution **: Use UnderGroundWeather API to pull the Historical weather. 
                Please refer to WeatherData.java to see the code. 




** Problem **: How to persist json file into mongo DB.

** Solution **: Use the Mongo DB build funtionlity to save json file.
                Please refer to DataStorage.java to see the code. 




** Problem **: How to calculate standard deviation on given data.

** Solution **: We have used the codes in Statistics.java to get find the standard deviation.
                Please refer to Statistics.java to see the code. 



## Reference
Source: 1. http://api.wunderground.com/api/86a9b3e36957b844/history_
        2. https://api.enigma.io/v2/data/



