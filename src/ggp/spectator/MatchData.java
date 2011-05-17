package ggp.spectator;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import javax.jdo.JDOObjectNotFoundException;
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

    public MatchData(JSONObject theMatchJSON, String authToken) throws IOException {
        this.matchKey = getNewKeyForJSON(theMatchJSON);
        this.theMatchJSON = new Text(theMatchJSON.toString());
        this.lastUpdated = new Date();        
        this.theAuthToken = authToken;
        this.theAtomEntries = new ArrayList<Text>();        
        this.theAtomEntries.add(new Text(getAtomEntry(theMatchJSON.toString(), lastUpdated)));
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
            chanserv.sendMessage(new ChannelMessage(clientID, theMatchJSON.getValue()));
        }
    }

    public void setMatchJSON(JSONObject theNewJSON) {
        this.theMatchJSON = new Text(theNewJSON.toString());
        this.lastUpdated = new Date();        
        this.theAtomEntries.add(new Text(getAtomEntry(theNewJSON.toString(), lastUpdated)));
    }

    public String getAtomEntry(String theJSON, Date lastUpdated) {
        StringBuilder b = new StringBuilder();
        String atomStateId = "tag:matches.ggp.org,2010-01-01:/matches/" + matchKey + "/" + Math.abs(theJSON.hashCode());
        b.append("   <entry> \n");
        b.append("      <title>State Transition at " + getAtomDateString(lastUpdated) + "</title> \n");
        b.append("      <link href=\"http://matches.ggp.org/matches/" + matchKey + "/\"/> \n");
        b.append("      <id>" + atomStateId + "</id> \n");
        b.append("      <updated>" + getAtomDateString(lastUpdated) + "</updated> \n");
        b.append("      <summary>State changed in underlying match.</summary> \n");
        b.append("   </entry> \n");  
        return b.toString();
    }

    public JSONObject getMatchJSON() {
        try {
            return new JSONObject(theMatchJSON.getValue());
        } catch (JSONException e) {
            return null;
        }
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

        String atomTitle = "GGP Match [" + matchKey + "]";        
        String atomId = "tag:matches.ggp.org,2010-01-01:/matches/" + matchKey + "/";        
        
        b.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?> \n");
        b.append("<feed xmlns=\"http://www.w3.org/2005/Atom\"> \n");
        b.append("   <title>" + atomTitle + "</title> \n");
        b.append("   <link href=\"http://pubsubhubbub.appspot.com/\" rel=\"hub\"/> \n");
        b.append("   <link href=\"http://matches.ggp.org/matches/" + matchKey + "/\"/> \n");
        b.append("   <link rel=\"self\" href=\"http://matches.ggp.org/matches/" + matchKey + "/feed.atom\"/> \n");
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
    public static MatchData loadMatchData(String matchKey) throws IOException {
        return Persistence.loadSpecific(matchKey, MatchData.class);
    }    
    
    private static String getNewKeyForJSON(JSONObject theJSON) throws IOException {
        String theKey;
        int nAttempt = -1;
        do {
            nAttempt++;
            theKey = computeKeyAttempt(theJSON, nAttempt);
        } while (theKey.length() > 0 && loadMatchData(theKey) != null);
        return theKey;
    }

    public static MatchData loadExistingMatchFromJSON(PersistenceManager pm, JSONObject theJSON) throws JDOObjectNotFoundException {
        int nAttempt = -1;
        while(true) {
            nAttempt++;
            String theKey = computeKeyAttempt(theJSON, nAttempt);
            if (theKey.length() == 0)
                throw new JDOObjectNotFoundException();

            MatchData m = (MatchData)pm.detachCopy(pm.getObjectById(MatchData.class, theKey));     
            try {
                JSONObject theOldJSON = m.getMatchJSON();
                if (!theOldJSON.getString("matchId").equals(theJSON.getString("matchId"))) continue;
                if (theOldJSON.getLong("startTime") != theJSON.getLong("startTime")) continue;
                if (!theOldJSON.getString("randomToken").equals(theJSON.getString("randomToken"))) continue;
                if (theOldJSON.has("matchHostPK") || theJSON.has("matchHostPK")) {
                    if (!theOldJSON.getString("matchHostPK").equals(theJSON.getString("matchHostPK")))
                        continue;
                }
                return m;
            } catch (JSONException e) {
                ;
            }            
        }
    }

    private static String computeKeyAttempt(JSONObject theJSON, int nKeyAttempt) {
        try {
            String theKey = computeHash(theJSON.getString("matchId"));
            theKey += "." + theJSON.getLong("startTime");
            theKey += "." + computeHash(theJSON.getString("randomToken"));
            if (theJSON.has("matchHostPK")) {
                theKey += "." + computeHash(theJSON.getString("matchHostPK"));
            }
            theKey += "-" + nKeyAttempt;
            return computeHash(theKey);
        } catch(JSONException e) {
            return "";
        }
    }

    // Computes the SHA1 hash of a given input string, and represents
    // that hash as a hexadecimal string.
    private static String computeHash(String theData) {
        try {
            MessageDigest SHA1 = MessageDigest.getInstance("SHA1");
    
            DigestInputStream theDigestStream = new DigestInputStream(
                    new BufferedInputStream(new ByteArrayInputStream(
                            theData.getBytes("UTF-8"))), SHA1);
            while (theDigestStream.read() != -1);
            byte[] theHash = SHA1.digest();
    
            Formatter hexFormat = new Formatter();
            for (byte x : theHash) {
                hexFormat.format("%02x", x);
            }
            return hexFormat.toString();
        } catch (Exception e) {
            return null;
        }
    }
}