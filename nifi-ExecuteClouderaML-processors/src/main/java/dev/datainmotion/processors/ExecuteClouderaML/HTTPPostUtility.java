package dev.datainmotion.processors.ExecuteClouderaML;

import org.apache.http.entity.ContentType;
import kong.unirest.*;
import kong.unirest.json.*;
import kong.unirest.apache.*;

import java.io.InputStream;

/**
 * @author tspann
 * http://kong.github.io/unirest-java/#requests
 */
public class HTTPPostUtility {


    /**
	 * call cloudera cml
     * @param urlName
     * @param accessKey
     * @param request
     */
    public static HTTPPostResults postToCML(String urlName, String accessKey, String request) {

		if ( urlName == null || accessKey == null || request == null || urlName.trim().length() <= 0 ||
				accessKey.trim().length()<= 0 || request.trim().length()<=0) {
			return null;
		}

		HTTPPostResults results = new HTTPPostResults();
		UnirestInstance unirest = Unirest.spawnInstance();

		if ( unirest == null) {
			unirest = Unirest.primaryInstance();
		}
		if (unirest == null)
		{
			return results;
		}

		try {
			unirest.config()
					.socketTimeout( 90000 )
					.connectTimeout( 180000 )
					.concurrency( 2, 2 )
					.setDefaultHeader( "Accept", "application/json" )
					.followRedirects( true )
					.verifySsl(false)
					.enableCookieManagement( true );

//			CMLPost cmlPost = new CMLPost();
//			cmlPost.setAccessKey( accessKey );
//			cmlPost.setRequest( request );

			//				.body(cmlPost.getAsJSON())
			HttpResponse<JsonNode> resp = unirest.post( urlName )
					.header( "accept", "application/json" )
					.header("Content-Type", "application/json")
					.body("{\"accessKey\":\"" + accessKey + "\",\"request\":{" + request + "}}")
					.asJson();

			if (resp.getBody() != null ) {
				results.setJsonResultBody(resp.getBody().toPrettyString());
			}

			if ( resp.getHeaders() != null) {
				results.setHeader( resp.getHeaders().toString() );
			}
			if ( resp.getStatusText() != null ) {
				results.setStatus(resp.getStatusText());
			}

			results.setStatusCode(resp.getStatus());
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			unirest.close();
			unirest.shutDown();
			unirest = null;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }
}
