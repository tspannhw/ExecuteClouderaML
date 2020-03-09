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

		Unirest.config().reset();
        Unirest.config()
                .socketTimeout( 90000 )
                .connectTimeout( 180000 )
                .concurrency( 10, 5 )
                .setDefaultHeader( "Accept", "application/json" )
                .followRedirects( true )
				.verifySsl(false)
                .enableCookieManagement( true );

		CMLPost cmlPost = new CMLPost();
		cmlPost.setAccessKey( accessKey );
		cmlPost.setRequest( request );

		HttpResponse<JsonNode> resp = Unirest.post( urlName )
                .header( "accept", "application/json" )
				.header("Content-Type", "application/json")
				.body(cmlPost.getAsJSON())
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

        try {
            Unirest.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }
}
