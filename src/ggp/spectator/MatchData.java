package ggp.spectator;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import com.google.appengine.api.channel.ChannelMessage;
import com.google.appengine.api.channel.ChannelService;
import com.google.appengine.api.channel.ChannelServiceFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

@PersistenceCapable
public class MatchData {
    @PrimaryKey @Persistent private String matchKey;
    @Persistent private String theAuthToken;
    @Persistent private Text theMatchJSON;
    
    @Persistent private List<Text> theAtomEntries;
    @Persistent private Date lastUpdated;
    
    @Persistent private Set<String> theClientIDs;
    
    public MatchData(String theMatchJSON, String authToken) throws IOException {
        this.matchKey = getKeyFromJSON(theMatchJSON);
        this.theMatchJSON = new Text(theMatchJSON);
        this.lastUpdated = new Date();        
        this.theAuthToken = authToken;
        this.theAtomEntries = new ArrayList<Text>();        
        this.theAtomEntries.add(new Text(getAtomEntry(theMatchJSON, lastUpdated)));
        this.theClientIDs = new HashSet<String>();

        if (matchKey.length() > 0) {
            PersistenceManager pm = Persistence.getPersistenceManager();
            pm.makePersistent(this);
            pm.close();
        }
    }
    
    public void addClientID(String clientID) {
        if (theClientIDs == null) theClientIDs = new HashSet<String>();
        theClientIDs.add(clientID);
    }
    
    public int numClientIDs() {
        return theClientIDs.size();
    }
    
    public void pingChannelClients() {
        if (theClientIDs == null) return;        
        ChannelService chanserv = ChannelServiceFactory.getChannelService();
        for(String clientID : theClientIDs) {
            chanserv.sendMessage(new ChannelMessage(clientID, getMatchJSON()));
        }
    }
    
    public void setMatchJSON(String theNewJSON) {
        this.theMatchJSON = new Text(theNewJSON);
        this.lastUpdated = new Date();        
        this.theAtomEntries.add(new Text(getAtomEntry(theNewJSON, lastUpdated)));
    }
    
    public static String getAtomEntry(String theJSON, Date lastUpdated) {
        String key = MatchData.getKeyFromJSON(theJSON);
        StringBuilder b = new StringBuilder();
        String atomStateId = "tag:matches.ggp.org,2010-01-01:/matches/" + key + "/" + Math.abs(theJSON.hashCode());
        b.append("   <entry> \n");
        b.append("      <title>State Transition at " + getAtomDateString(lastUpdated) + "</title> \n");
        b.append("      <link href=\"http://matches.ggp.org/matches/" + key + "/\"/> \n");
        b.append("      <id>" + atomStateId + "</id> \n");
        b.append("      <updated>" + getAtomDateString(lastUpdated) + "</updated> \n");
        b.append("      <summary>State changed in underlying match.</summary> \n");
        b.append("   </entry> \n");  
        return b.toString();
    }
    
    public String getMatchJSON() {
        return theMatchJSON.getValue();
    }
    
    public String getAuthToken() {
        return theAuthToken;
    }
    
    public String getMatchKey() {
        return matchKey;
    }
    
    // Based on RFC 3339
    public static String getAtomDateString(Date d) {
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormatGmt.format(d);
    }
    
    public String getAtomFeed() {
        StringBuilder b = new StringBuilder();

        String key = MatchData.getKeyFromJSON(theMatchJSON.getValue());
        String atomTitle = "GGP Match [" + key + "]";        
        String atomId = "tag:matches.ggp.org,2010-01-01:/matches/" + key + "/";        
        
        b.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?> \n");
        b.append("<feed xmlns=\"http://www.w3.org/2005/Atom\"> \n");
        b.append("   <title>" + atomTitle + "</title> \n");
        b.append("   <link href=\"http://pubsubhubbub.appspot.com/\" rel=\"hub\"/> \n");
        b.append("   <link href=\"http://matches.ggp.org/matches/" + key + "/\"/> \n");
        b.append("   <link rel=\"self\" href=\"http://matches.ggp.org/matches/" + key + "/feed.atom\"/> \n");
        b.append("   <updated>" + getAtomDateString(lastUpdated) + "</updated> \n");
        b.append("   <author><name>GGP Spectator Server</name></author> \n");
        b.append("   <id>" + atomId + "</id> \n");
        b.append("\n");
        for(int i = theAtomEntries.size()-1; i >= 0; i--) {
            b.append(theAtomEntries.get(i).getValue());
        }
        b.append("</feed> \n");
        
        return b.toString();
    }
        
    /* Static accessor methods */
    public static Set<MatchData> loadMatches() throws IOException {
        return Persistence.loadAll(MatchData.class);
    }
    
    public static MatchData loadMatchData(String matchKey) throws IOException {
        return Persistence.loadSpecific(matchKey, MatchData.class);
    }
    
    public static void clearMatches() throws IOException {
        Persistence.clearAll(MatchData.class);
    }
    
    public static String getKeyFromJSON(String theMatchJSON) {
        try {
            JSONObject theJSON = new JSONObject(theMatchJSON.toString());
            String theKey = theJSON.getString("matchId");
            theKey += "." + theJSON.getLong("startTime");
            theKey += "." + theJSON.getString("randomToken");
            if (theJSON.has("matchHostPK")) {
                theKey += "." + theJSON.getString("matchHostPK");
            }
            return theKey;
        } catch(JSONException e) {
            return "";
        }
    }
    
    public static String getRepositoryServerFromJSON(String theMatchJSON) {
        try {
            JSONObject theJSON = new JSONObject(theMatchJSON.toString());
            String theGameURL = theJSON.getString("gameMetaURL");
            return new URL(theGameURL).getHost();
        } catch(MalformedURLException e) {
            return "";
        } catch(JSONException e) {
            return "";
        }
    }
    
    public static String getRandomString(int nLength) {
        Random theGenerator = new Random();
        String theString = "";
        for (int i = 0; i < nLength; i++) {
            int nVal = theGenerator.nextInt(62);
            if (nVal < 26) theString += (char)('a' + nVal);
            else if (nVal < 52) theString += (char)('A' + (nVal-26));
            else if (nVal < 62) theString += (char)('0' + (nVal-52));
        }
        return theString;
    }    
}