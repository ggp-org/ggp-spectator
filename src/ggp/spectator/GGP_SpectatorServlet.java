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
        } else {
            if (showFeedView) {
                resp.setContentType("application/atom+xml");
                resp.getWriter().println(theMatch.getAtomFeed());
            } else {
                resp.getWriter().println(theMatch.getMatchJSON());
            }
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
            
            if (oldMatchJSON.has("isCompleted") && newMatchJSON.has("isCompleted") && oldMatchJSON.getBoolean("isCompleted") && !newMatchJSON.getBoolean("isCompleted")) {
                throw new IOException("Cannot transition from completed to not-completed.");
            }
            if (oldMatchJSON.getJSONArray("states").length() > newMatchJSON.getJSONArray("states").length()) {
                throw new IOException("Number of states decreased from " + oldMatchJSON.getJSONArray("states").length() + " to " + newMatchJSON.getJSONArray("states").length() + " during update: not allowed.");
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
        } catch(JSONException e) {
            throw new IOException("Could not parse JSON: " + e.toString());
        }
    }
    
    public void verifyHas(JSONObject obj, String k) throws IOException {
        if (!obj.has(k)) {
            throw new IOException("Could not find required field " + k);
        }
    }
    
    public void verifyEquals(JSONObject old, JSONObject newer, String k) throws IOException {
        try {
            if (!old.has(k) && !newer.has(k)) {
                return;
            } else if (!old.has(k)) {
                throw new IOException("Incompability for " + k + ": old has null, new has [" + newer.get(k) + "].");
            } else if (!old.get(k).equals(newer.get(k))) {
                throw new IOException("Incompability for " + k + ": old has [" + old.get(k) + "], new has [" + newer.get(k) + "].");
            }
        } catch (JSONException e) {
            throw new IOException("JSON parsing exception: " + e.toString());
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
            try {
                if (theMatch.getMatchJSON().getBoolean("isCompleted")) {
                    resp.getWriter().write("Match [" + theKey + "] already completed; no need to register.");
                    throw new JDOObjectNotFoundException();
                }
            } catch (JSONException e2) {
            }
            theMatch.addClientID(theClientID);
            pm.makePersistent(theMatch);
            resp.getWriter().write("Registered for [" + theKey + "] as [" + theClientID + "].");
        } catch(JDOObjectNotFoundException e) {
            ;
        } finally {
            pm.close();
        }
    }
    
    // ========================================================================
    // Generically useful utility functions.
    
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