package ggp.spectator;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

public class PuSHPublisher {    
    public static void pingHub(String theHub, String theFeedURL) throws IOException {
        String theEncodedURL = URLEncoder.encode(theFeedURL, "UTF-8");

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
            } else {
                throw new IOException(connection.getResponseCode() + ": " + connection.getResponseMessage());
            }
        } catch (MalformedURLException e) {
            throw new IOException(e);
        } catch (IOException e) {
            throw e;
        }
    }
}