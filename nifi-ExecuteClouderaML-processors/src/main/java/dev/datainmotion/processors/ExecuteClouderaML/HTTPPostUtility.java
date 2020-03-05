package dev.datainmotion.processors.ExecuteClouderaML;

import org.apache.http.entity.ContentType;
import kong.unirest.*;
import kong.unirest.json.*;
import kong.unirest.apache.*;
import java.io.InputStream;

/**
 * 
 * @author tspann
 * http://kong.github.io/unirest-java/#requests
 */
public class HTTPPostUtility {


   public static void postToCML(String urlName, String accessKey, String request) {
        Unirest.config()
           .socketTimeout(500)
           .connectTimeout(1000)
           .concurrency(10, 5)
           .setDefaultHeader("Accept", "application/json")
           .followRedirects(true)
           .enableCookieManagement(true);

     HttpResponse<JsonNode> response = Unirest.post("http://httpbin.org/post")
      .header("accept", "application/json")
      .queryString("apiKey", "123")
      .field("parameter", "value")
      .field("firstname", "Gary")
      .asJson();

      System.out.println("response: " + response.toString()); 
      //Unirest.shutdown();
   }
	/**
	 * postImage
	 * 
	 * @param urlName
	 *            name of URL to post to
	 * @param fieldName
	 *            data or whatever your server needs. this example works with mms
	 *            model server
	 * @param imageName
	 *            make this a real name like test.jpg
	 * @param imageType
	 *            must be a valid content type like image/jpeg
	 * @param stream
	 *            InputStream of image / flowfile
	 * @return JSON results from the POST
	 
	public static HTTPPostResults postImage(String urlName, String fieldName, String imageName, String imageType,
											InputStream stream, String headerName, String headerValue,
											String basicUsername, String basicPassword) {

		if ( urlName == null || fieldName == null || imageName == null || imageType == null || stream == null ) {
			return null;
		}
		
		HTTPPostResults results = new HTTPPostResults();

		try {
			// do we want to allow users to set this
			// connectionTimeout
			// connectionTimeout
			// http://unirest.io/java.html

			 Need base auth

			 .basicAuth("username", "password")

			 add

			 headers

			 .header("accept", "application/json")

			Unirest.setTimeouts(90000, 180000);
			
			HttpResponse<JsonNode> resp = null;

			if (headerName == null || headerName.length() <= 0 || headerValue == null || headerValue.length() <= 0) {
				resp = Unirest.post(urlName)
						.field(fieldName, stream, ContentType.parse(imageType), imageName)
						.asJson();
			}
			else {
				resp = Unirest.post(urlName)
						.header(headerName,headerValue)
						.field(fieldName, stream, ContentType.parse(imageType), imageName)
						.asJson();
			}

			if (resp.getBody() != null && resp.getBody().getArray() != null && resp.getBody().getArray().length() > 0) {
				for (int i = 0; i < resp.getBody().getArray().length(); i++) {
					if (resp.getBody().getArray().get(i) != null) {
						results.setJsonResultBody(resp.getBody().getArray().get(i).toString());
					}
				}
			}

			if ( resp.getHeaders() != null) { 
				results.setHeader( resp.getHeaders().toString() );
			}
			if ( resp.getStatusText() != null ) { 
				results.setStatus(resp.getStatusText());
			}
			
			results.setStatusCode(resp.getStatus());
			
//			try {
//				Unirest.shutdown();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
		} catch (Throwable t) {
			t.printStackTrace();
		}

		return results;
	} **/
}
