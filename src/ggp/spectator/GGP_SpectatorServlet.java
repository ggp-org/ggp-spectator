package ggp.spectator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Random;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.servlet.http.*;

import com.google.appengine.api.channel.ChannelServiceFactory;
import com.google.appengine.repackaged.org.json.JSONArray;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

@SuppressWarnings("serial")
public class GGP_SpectatorServlet extends HttpServlet {
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");            

        // Get the requested URL, and make sure that it starts with
        // the expected "/matches/" prefix.
        String theURL = req.getRequestURI();
        resp.setContentType(getContentType(theURL));
        if(!theURL.startsWith("/matches/")) {
            // TODO: Add a proper splash page, etc. For now, we reject any URL
            // that doesn't begin with /matches/.
            resp.setStatus(404);
            return;
        }
        
        // Strip off the initial "/matches/" prefix.
        theURL = theURL.substring("/matches/".length());
        if(theURL.endsWith("/")) {
            theURL = theURL.substring(0, theURL.length()-1);
        }
        
        // If they want a match visualization page, provide that without
        // needing to look up the match key or anything, since it'll be
        // available via the "." relative path.
        if(theURL.endsWith("/viz.html")) {
            writeStaticPage(resp, "MatchPage.html");
            return;
        }        

        // If they're requesting a channel token, we can handle
        // that immediately without needing further parsing.
        if(theURL.equals("channel.js")) {
            writeChannelToken(resp);
            return;            
        }

        // If they're using a channel token to register for updates on a
        // particular match, we can handle that immediately by parsing out
        // the match key and the channel token.
        if(theURL.endsWith("/channel.js")) {
            theURL = theURL.substring(0, theURL.length() - 11);
            if (theURL.contains("/clientId=")) {
                String theID = theURL.substring(theURL.indexOf("/clientId=")+("/clientId=".length()));
                String theKey = theURL.substring(0, theURL.indexOf("/clientId="));
                registerChannelForMatch(resp, theKey, theID);
            } else {
                resp.setStatus(404);
            }
            return;
        }

        boolean showFeedView = false;
        if(theURL.endsWith("/feed.atom")) {
            showFeedView = true;
            theURL = theURL.substring(0, theURL.length()-10);
        }

        MatchData theMatch = MatchData.loadMatchData(theURL);
        if (theMatch == null) {
            resp.setStatus(404);
            return;
        }
        
        if (showFeedView) {
            resp.getWriter().println(theMatch.getAtomFeed());
        } else {
            resp.getWriter().println(theMatch.getMatchJSON());
        }
    }

    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {        
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");            
        
        String theURL = req.getRequestURI();
        if(!theURL.equals("/"))
            return;

        JSONObject theMatchJSON;
        String theAuthToken = req.getParameter("AUTH");
        try {
            theMatchJSON = new JSONObject(req.getParameter("DATA"));
            String theRepository = new URL(theMatchJSON.getString("gameMetaURL")).getHost();
            if (!theRepository.equals("ggp-repository.appspot.com") &&
                !theRepository.equals("games.ggp.org")) {
                // TODO: Make this more permissive. What's the best way to do this
                // while still providing security for viewers?
                throw new IOException("Repository not whitelisted: " + theRepository);
            }
        } catch (JSONException e) {
            throw new IOException(e);
        }

        MatchData theMatch = null;
        PersistenceManager pm = Persistence.getPersistenceManager();
        try {
            theMatch = MatchData.loadExistingMatchFromJSON(pm, theMatchJSON);
            if (!theMatch.getAuthToken().equals(theAuthToken)) {
                throw new IOException("Unauthorized auth token used to update match.");
            }
            performUpdateValidationChecks(theMatch.getMatchJSON(), theMatchJSON);
            performInternalConsistencyChecks(theMatchJSON);
            theMatch.setMatchJSON(theMatchJSON);
            pm.makePersistent(theMatch);
        } catch(JDOObjectNotFoundException e) {
            performCreationValidationChecks(theMatchJSON);
            performInternalConsistencyChecks(theMatchJSON);
            theMatch = new MatchData(theMatchJSON, theAuthToken);;
        } finally {
            pm.close();
        }

        // Respond to the match host as soon as possible.
        resp.getWriter().println(theMatch.getMatchKey());
        resp.getWriter().close();
        
        // Ping the channel clients and the PuSH hub.
        theMatch.pingChannelClients();
        PuSHPublisher.pingHub("http://pubsubhubbub.appspot.com/", "http://matches.ggp.org/matches/" + theMatch.getMatchKey() + "/feed.atom");
    }
    
    public void doOptions(HttpServletRequest req, HttpServletResponse resp) throws IOException {  
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");    
    }
    
    // =======================================================================
    // Validation checks, to make sure bad information is not published to the
    // spectator server by a malicious adversary or malfunctioning system. These
    // are not comprehensive, but are intended to provide basic sanity guarantees
    // so that we aren't storing total nonsense in the spectator server.
    
    public void performCreationValidationChecks(JSONObject newMatchJSON) throws IOException {
        try {
            long startTime = newMatchJSON.getLong("startTime");
            if (startTime < 1000000000000L || startTime > 2000000000000L) {
                throw new IOException("Unreasonable start time.");
            }
            
            String randomToken = newMatchJSON.getString("randomToken");
            if (randomToken.length() < 12) {
                throw new IOException("Random token is too short.");
            }
        } catch(JSONException e) {
            throw new IOException("Could not parse JSON: " + e.toString());
        }
    }
    
    public void performUpdateValidationChecks(JSONObject oldMatchJSON, JSONObject newMatchJSON) throws IOException {
        try {
            verifyEquals(oldMatchJSON, newMatchJSON, "matchId");
            verifyEquals(oldMatchJSON, newMatchJSON, "startTime");
            verifyEquals(oldMatchJSON, newMatchJSON, "randomToken");
            verifyEquals(oldMatchJSON, newMatchJSON, "matchHostPK");
            verifyEquals(oldMatchJSON, newMatchJSON, "startClock");
            verifyEquals(oldMatchJSON, newMatchJSON, "playClock");                
            verifyEquals(oldMatchJSON, newMatchJSON, "gameMetaURL");
            verifyEquals(oldMatchJSON, newMatchJSON, "gameName");
            verifyEquals(oldMatchJSON, newMatchJSON, "gameRulesheetHash");
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "gameRoleNames", false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "playerNamesFromHost", false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "moves", true);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "errors", true);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "states", true);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "stateTimes", true);            

            if (oldMatchJSON.has("isCompleted") && newMatchJSON.has("isCompleted") && oldMatchJSON.getBoolean("isCompleted") && !newMatchJSON.getBoolean("isCompleted")) {
                throw new IOException("Cannot transition from completed to not-completed.");
            }            
        } catch(JSONException e) {
            throw new IOException("Could not parse JSON: " + e.toString());
        }
    }
    
    public void performInternalConsistencyChecks(JSONObject theMatchJSON) throws IOException {
        try {
            verifyHas(theMatchJSON, "matchId");
            verifyHas(theMatchJSON, "startTime");
            verifyHas(theMatchJSON, "randomToken");
            verifyHas(theMatchJSON, "startClock");
            verifyHas(theMatchJSON, "playClock");
            verifyHas(theMatchJSON, "states");
            verifyHas(theMatchJSON, "moves");
            verifyHas(theMatchJSON, "stateTimes");
            verifyHas(theMatchJSON, "gameMetaURL");

            int movesLength = theMatchJSON.getJSONArray("moves").length();
            int statesLength = theMatchJSON.getJSONArray("states").length();
            int stateTimesLength = theMatchJSON.getJSONArray("stateTimes").length();
            if (statesLength != stateTimesLength) {
                throw new IOException("There are " + statesLength + " states, but " + stateTimesLength + " state times. Inconsistent!");
            }
            if (statesLength != movesLength+1) {
                throw new IOException("There are " + statesLength + " states, but " + movesLength + " moves. Inconsistent!");
            }

            long theTime = theMatchJSON.getLong("startTime");
            verifyReasonableTime(theMatchJSON.getLong("startTime"));
            for (int i = 0; i < stateTimesLength; i++) {
                verifyReasonableTime(theMatchJSON.getJSONArray("stateTimes").getLong(i));
                if (theTime > theMatchJSON.getJSONArray("stateTimes").getLong(i)) {
                    throw new IOException("Time sequence goes backward!");
                } else {
                    theTime = theMatchJSON.getJSONArray("stateTimes").getLong(i);
                }
            }
        } catch(JSONException e) {
            throw new IOException("Could not parse JSON: " + e.toString());
        }
    }
    
    public void verifyReasonableTime(long theTime) throws IOException {
        if (theTime < 0) throw new IOException("Time is negative!");
        if (theTime < 1200000000000L) throw new IOException("Time is before GGP Galaxy began.");
        if (theTime > System.currentTimeMillis() + 604800000L) throw new IOException("Time is after a week from now.");        
    }
    
    public void verifyHas(JSONObject obj, String k) throws IOException {
        if (!obj.has(k)) {
            throw new IOException("Could not find required field " + k);
        }
    }
    
    public void verifyEquals(JSONObject old, JSONObject newer, String k) throws JSONException, IOException {
        if (!old.has(k) && !newer.has(k)) {
            return;
        } else if (!old.has(k)) {
            throw new IOException("Incompability for " + k + ": old has null, new has [" + newer.get(k) + "].");
        } else if (!old.get(k).equals(newer.get(k))) {
            throw new IOException("Incompability for " + k + ": old has [" + old.get(k) + "], new has [" + newer.get(k) + "].");
        }
    }
    
    public void verifyOptionalArraysEqual(JSONObject old, JSONObject newer, String arr, boolean arrayCanExpand) throws JSONException, IOException {
        if (!old.has(arr) && !newer.has(arr)) return;
        if (old.has(arr) && !newer.has(arr)) throw new IOException("Array " + arr + " missing from new, present in old.");
        if (!old.has(arr) && newer.has(arr)) return; // okay for the array to appear mid-way through the game
        JSONArray oldArr = old.getJSONArray(arr);
        JSONArray newArr = newer.getJSONArray(arr);
        if (!arrayCanExpand && oldArr.length() != newArr.length()) throw new IOException("Array " + arr + " has length " + newArr.length() + " in new, length " + oldArr.length() + " in old.");
        if (newArr.length() < oldArr.length()) throw new IOException("Array " + arr + " shrank from length " + oldArr.length() + " to length " + newArr.length() + ".");
        for (int i = 0; i < oldArr.length(); i++) {
            if (oldArr.get(i) instanceof JSONArray) {
                if (!(newArr.get(i) instanceof JSONArray))
                    throw new IOException("Array " + arr + " used to have interior arrays but no longer does, in position " + i + ".");
                if(!oldArr.getJSONArray(i).toString().equals(newArr.getJSONArray(i).toString()))
                    throw new IOException("Array " + arr + " has disagreement beween new [" + newArr.get(i) + "] and old [" + oldArr.get(i) + "] at element " + i + ".");
            } else if (!oldArr.get(i).equals(newArr.get(i))) {
                throw new IOException("Array " + arr + " has disagreement beween new [" + newArr.get(i) + "] and old [" + oldArr.get(i) + "] at element " + i + ".");
            }
        }
    }    
    
    // ========================================================================
    // Channel token handling: we need to be able to create channel tokens/IDs,
    // and register those IDs with particular matches so that we can push updates
    // to the appropriate channels whenever a particular match is updated.
    // Note that creation of channel token/IDs is done centrally: you request a
    // channel token/ID, then you register it with particular matches. This lets
    // you get updates about multiple matches in the same browser session.

    public void writeChannelToken(HttpServletResponse resp) throws IOException {        
        String theClientID = getRandomString(32);
        String theToken = ChannelServiceFactory.getChannelService().createChannel(theClientID);
        resp.getWriter().println("theChannelID = \"" + theClientID + "\";\n");
        resp.getWriter().println("theChannelToken = \"" + theToken + "\";\n");
    }    

    public void registerChannelForMatch(HttpServletResponse resp, String theKey, String theClientID) throws IOException {
        MatchData theMatch = null;
        PersistenceManager pm = Persistence.getPersistenceManager();
        try {
            theMatch = pm.detachCopy(pm.getObjectById(MatchData.class, theKey));
            if (theMatch.addClientID(theClientID)) {
                pm.makePersistent(theMatch);
                resp.getWriter().write("Registered for [" + theKey + "] as [" + theClientID + "].");
            } else {
                resp.getWriter().write("Match [" + theKey + "] already completed; no need to register.");
            }
        } catch(JDOObjectNotFoundException e) {
            ;
        } finally {
            pm.close();
        }
    }
    
    // ========================================================================
    // Generically useful utility functions.

    private static String getContentType(String theURL) {
        if (theURL.endsWith(".xml")) {
            return "application/xml";
        } else if (theURL.endsWith(".xsl")) {
            return "application/xml";
        } else if (theURL.endsWith(".js")) {
            return "text/javascript";   
        } else if (theURL.endsWith(".json")) {
            return "text/javascript";
        } else if (theURL.endsWith(".html")) {
            return "text/html";
        } else if (theURL.endsWith(".png")) {
            return "image/png";
        } else if (theURL.endsWith(".atom")) {
            return "application/atom+xml";
        } else {
            if (theURL.equals("/")) {
                return "text/html";
            } else if (theURL.endsWith("/")) {
                return "text/javascript";
            }
            
            return "text/plain";
        }
    }    
    
    public void writeStaticPage(HttpServletResponse resp, String thePage) throws IOException {
        FileReader fr = new FileReader("root/" + thePage);
        BufferedReader br = new BufferedReader(fr);
        StringBuffer response = new StringBuffer();
        
        String line;
        while( (line = br.readLine()) != null ) {
            response.append(line + "\n");
        }
        
        resp.setContentType("text/html");
        resp.getWriter().println(response.toString());
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