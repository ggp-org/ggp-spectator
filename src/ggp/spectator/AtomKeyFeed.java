package ggp.spectator;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import org.ggp.galaxy.shared.persistence.Persistence;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

@PersistenceCapable
public class AtomKeyFeed {
    @PrimaryKey @Persistent private String theFeedKey;
    @Persistent private List<String> recentMatchEntries_Key;
    @Persistent private List<Date> recentMatchEntries_Date;
    @Persistent private List<Integer> recentMatchEntries_Index;
    @Persistent private Date lastUpdated;
    @Persistent private int nCount;
    private static final int kRecentMatchKeysToRecord = 100;
    
    public AtomKeyFeed(String theFeedKey) {
        this.theFeedKey = theFeedKey;
        this.recentMatchEntries_Key = new ArrayList<String>();
        this.recentMatchEntries_Date = new ArrayList<Date>();
        this.recentMatchEntries_Index = new ArrayList<Integer>();
        this.lastUpdated = new Date();
        this.nCount = 0;
    }
    
    /* Static accessor methods */
    public static void addRecentMatchKey(String theFeedKey, String theMatchKey) {
        PersistenceManager pm = Persistence.getPersistenceManager();
        AtomKeyFeed recent = null;
        try {
            recent = pm.getObjectById(AtomKeyFeed.class, theFeedKey);
        } catch (JDOObjectNotFoundException onfe) {
            recent = new AtomKeyFeed(theFeedKey);
        }
        
        recent.nCount += 1;        
        recent.lastUpdated = new Date();
        recent.recentMatchEntries_Key.add(theMatchKey);
        recent.recentMatchEntries_Date.add(new Date());
        recent.recentMatchEntries_Index.add(recent.nCount);
        if (recent.recentMatchEntries_Key.size() > kRecentMatchKeysToRecord) {
            recent.recentMatchEntries_Key.remove(0);
            recent.recentMatchEntries_Date.remove(0);
            recent.recentMatchEntries_Index.remove(0);
        }
        pm.makePersistent(recent);
        pm.close();
    }
    
    public static List<String> getRecentMatchKeys(String theFeedKey) {
        AtomKeyFeed recent = Persistence.loadSpecific(theFeedKey, AtomKeyFeed.class);
        if (recent == null) return null;
        ArrayList<String> theRecentKeys = new ArrayList<String>();
        for (String s : recent.recentMatchEntries_Key) {
            theRecentKeys.add(s);
        }
        return theRecentKeys;
    }
    
    public static String getAtomFeed(String theFeedKey) {
        AtomKeyFeed recent = Persistence.loadSpecific(theFeedKey, AtomKeyFeed.class);
        if (recent == null) return null;
        return recent.getAtomFeed();        
    }
    
    public static String getJsonFeed(String theFeedKey) {
        AtomKeyFeed recent = Persistence.loadSpecific(theFeedKey, AtomKeyFeed.class);
        if (recent == null) return null;
        return recent.getJsonFeed();        
    }    
    
    /* ATOM Methods */
    public String getAtomFeed() {
        StringBuilder b = new StringBuilder();
        String atomTitle = "GGP Spectator Server Feed [" + theFeedKey + "]";
        String atomId = "tag:matches.ggp.org,2010-01-01:/matches/feeds/" + theFeedKey + ".atom";

        b.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?> \n");
        b.append("<feed xmlns=\"http://www.w3.org/2005/Atom\"> \n");
        b.append("   <title>" + atomTitle + "</title> \n");
        b.append("   <link rel=\"hub\" href=\"http://pubsubhubbub.appspot.com/\"/> \n");
        b.append("   <link rel=\"self\" href=\"http://matches.ggp.org/matches/feeds/" + theFeedKey + ".atom\" type=\"application/atom+xml\"/> \n");
        b.append("   <updated>" + getAtomDateString(lastUpdated) + "</updated> \n");
        b.append("   <author><name>GGP Spectator Server</name></author> \n");
        b.append("   <id>" + atomId + "</id> \n");
        b.append("\n");
        for (int i = recentMatchEntries_Key.size()-1; i >= 0; i--) {
            String theKey = recentMatchEntries_Key.get(i);
            Date theDate = recentMatchEntries_Date.get(i);
            int theIndex = recentMatchEntries_Index.get(i);
            String atomStateId = "tag:matches.ggp.org,2010-01-01:/matches/feeds/" + theFeedKey + ".atom/" + theIndex;
            String atomStateTime = getAtomDateString(theDate);
            b.append("   <entry> \n");
            b.append("      <title>Match</title> \n");
            b.append("      <link href=\"http://matches.ggp.org/matches/" + theKey + "/\"/> \n");
            b.append("      <id>" + atomStateId + "</id> \n");
            b.append("      <updated>" + atomStateTime + "</updated> \n");
            b.append("      <summary>Match event occurred.</summary> \n");
            b.append("   </entry> \n");  
        }
        b.append("</feed> \n");
        return b.toString();     
    }
    
    public static String getAtomDateString(Date d) {
        // Based on RFC 3339
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormatGmt.format(d);
    }
    
    /* JSON Methods */
    public String getJsonFeed() {
        JSONObject theFeed = new JSONObject();
        JSONArray theArray = new JSONArray();
        try {
            for (int i = recentMatchEntries_Key.size()-1; i >= 0; i--) {
                theArray.put(recentMatchEntries_Key.get(i));
            }
            theFeed.put(theFeedKey, theArray);
            return theFeed.toString();
        } catch (JSONException je) {
            return null;
        }
    }    
}