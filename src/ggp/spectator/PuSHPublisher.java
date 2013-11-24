package ggp.spectator;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.logging.Logger;

public class PuSHPublisher {
	private static final int PING_ATTEMPTS = 10;
    public static void pingHub(String theHub, String theFeedURL) throws IOException {
        String theEncodedURL = URLEncoder.encode(theFeedURL, "UTF-8");

        int nAttempts = 0;
        while(true) {
	        try {
	            URL url = new URL(theHub);
	            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	            connection.setDoOutput(true);
	            connection.setRequestMethod("POST");
	            connection.setRequestProperty("Content-type","application/x-www-form-urlencoded");
	            connection.setRequestProperty("User-agent", "pubsubhubbub 0.3");
	            
	            OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
	            writer.write("hub.mode=publish&hub.url=" + theEncodedURL);
	            writer.close();

	            if (connection.getResponseCode() / 100 == 2) {
	                // OK
	            	return;
	            } else {
	            	Logger.getAnonymousLogger().severe("Got bad response from ping PuSH hub: " + connection.getResponseCode() + ": " + connection.getResponseMessage());
	                throw new IOException(connection.getResponseCode() + ": " + connection.getResponseMessage());
	            }
	        } catch (IOException e) {
	        	if (nAttempts > PING_ATTEMPTS) {
	        		throw e;
	        	} else {
	        		Logger.getAnonymousLogger().severe("Failed to ping PuSH hub: " + e.toString() + " ... " + e.getCause());
	        	}
	        }
	        ++nAttempts;
        }
    }
}