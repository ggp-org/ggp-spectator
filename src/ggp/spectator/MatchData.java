package ggp.spectator;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import com.google.appengine.api.datastore.Text;

import external.JSON.JSONArray;
import external.JSON.JSONException;
import external.JSON.JSONObject;

import org.ggp.base.util.crypto.BaseHashing;
import org.ggp.galaxy.shared.persistence.Persistence;

@PersistenceCapable
public class MatchData {
    @PrimaryKey @Persistent private String matchKey;
    @Persistent private String theAuthToken;
    @Persistent private Text theMatchJSON;
    @Persistent private Date lastUpdated;

    public MatchData(JSONObject theMatchJSON, String authToken) throws IOException {
        this.matchKey = getNewKeyForJSON(theMatchJSON);
        this.theMatchJSON = new Text(theMatchJSON.toString());
        this.lastUpdated = new Date();
        this.theAuthToken = authToken;        
        
        if (matchKey.length() > 0) {
            PersistenceManager pm = Persistence.getPersistenceManager();
            pm.makePersistent(this);
            pm.close();
        }
    }
    
    public void setMatchJSON(JSONObject theNewJSON) {
        this.theMatchJSON = new Text(theNewJSON.toString());
        this.lastUpdated = new Date();
    }

    public JSONObject getMatchJSON() {
        try {
            return new JSONObject(theMatchJSON.getValue());
        } catch (JSONException e) {
            return null;
        }
    }
    
    public Date getLastUpdated() {
        // NOTE: For some matches this can be NULL, since they were added
        // before lastUpdated dates were properly recorded.
        return lastUpdated;
    }

    public String getAuthToken() {
        return theAuthToken;
    }

    public String getMatchKey() {
        return matchKey;
    }

    /* ATOM feed specific methods */    
    public String getAtomFeed() {
        try {
            StringBuilder b = new StringBuilder();
            JSONObject theJSON = new JSONObject(theMatchJSON.getValue());
            JSONArray theStates = theJSON.getJSONArray("states");
            JSONArray theStateTimes = theJSON.getJSONArray("stateTimes");
            
            String atomTitle = "GGP Match [" + matchKey + "]";
            String atomId = "tag:matches.ggp.org,2010-01-01:/matches/" + matchKey + "/";

            b.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?> \n");
            b.append("<feed xmlns=\"http://www.w3.org/2005/Atom\"> \n");
            b.append("   <title>" + atomTitle + "</title> \n");
            b.append("   <link href=\"http://pubsubhubbub.appspot.com/\" rel=\"hub\"/> \n");
            b.append("   <link href=\"http://matches.ggp.org/matches/" + matchKey + "/\"/> \n");
            b.append("   <link rel=\"self\" href=\"http://matches.ggp.org/matches/" + matchKey + "/feed.atom\"/> \n");
            b.append("   <updated>" + getAtomDateString(new Date(theStateTimes.getLong(theStateTimes.length()-1))) + "</updated> \n");
            b.append("   <author><name>GGP Spectator Server</name></author> \n");
            b.append("   <id>" + atomId + "</id> \n");
            b.append("\n");
            for (int i = theStates.length()-1; i >= 0; i--) {
                String atomStateId = "tag:matches.ggp.org,2010-01-01:/matches/" + matchKey + "/" + i;
                String atomStateTime = getAtomDateString(new Date(theStateTimes.getLong(i)));
                b.append("   <entry> \n");
                b.append("      <title>State Transition at " + atomStateTime + "</title> \n");
                b.append("      <link href=\"http://matches.ggp.org/matches/" + matchKey + "/\"/> \n");
                b.append("      <id>" + atomStateId + "</id> \n");
                b.append("      <updated>" + atomStateTime + "</updated> \n");
                b.append("      <summary>State changed in underlying match.</summary> \n");
                b.append("   </entry> \n");  
            }
            b.append("</feed> \n");
            
            return b.toString();
        } catch (JSONException e) {
            return null;
        }        
    }
    
    public static String getAtomDateString(Date d) {
        // Based on RFC 3339
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormatGmt.format(d);
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
            String theKey = BaseHashing.computeSHA1Hash(theJSON.getString("matchId"));
            theKey += "." + theJSON.getLong("startTime");
            theKey += "." + BaseHashing.computeSHA1Hash(theJSON.getString("randomToken"));
            if (theJSON.has("matchHostPK")) {
                theKey += "." + BaseHashing.computeSHA1Hash(theJSON.getString("matchHostPK"));
            }
            theKey += "-" + nKeyAttempt;
            return BaseHashing.computeSHA1Hash(theKey);
        } catch(JSONException e) {
            return "";
        }
    }
}