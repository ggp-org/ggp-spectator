package ggp.spectator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

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
        
        boolean showFeedView = false;
        String theURL = req.getRequestURI();
        if(!theURL.startsWith("/matches/")) {
            // Add a proper splash page, etc.
            resp.setStatus(404);
            return;
        }
        
        theURL = theURL.substring("/matches/".length());
        if(theURL.endsWith("/")) theURL = theURL.substring(0, theURL.length()-1);
        if(theURL.endsWith("/feed.atom")) {
            showFeedView = true;
            theURL = theURL.substring(0, theURL.length()-10);            
        }
        if(theURL.equals("channel.js")) {
            writeChannelToken(resp);
            return;            
        }
        if(theURL.endsWith("/channel.js")) {
            theURL = theURL.substring(0, theURL.length() - 11);
            if (theURL.contains("/clientId=")) {
                String theID = theURL.substring(theURL.indexOf("/clientId=")+10);
                theURL = theURL.substring(0, theURL.indexOf("/clientId="));
                registerChannelForMatch(resp, theURL, theID);
            } else {
                resp.setStatus(404);
            }
            return;
        }
        if(theURL.endsWith("/viz.html")) {
            writeStaticPage(resp, "MatchPage.html");
            return;
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

    public void writeStaticPage(HttpServletResponse resp, String thePage)
            throws IOException {
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
    
    public void writeChannelToken(HttpServletResponse resp) throws IOException {        
        String theClientID = MatchData.getRandomString(32);
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
                if (new JSONObject(theMatch.getMatchJSON()).getBoolean("isCompleted")) {
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
    
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {        
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");            
        
        String theURL = req.getRequestURI();
        if(!theURL.equals("/"))
            return;
        
        String theAuthToken = req.getParameter("AUTH");
        String theMatchJSON = req.getParameter("DATA");
        
        String theRepository = MatchData.getRepositoryServerFromJSON(theMatchJSON);
        if (!theRepository.equals("ggp-repository.appspot.com") &&
            !theRepository.equals("games.ggp.org")) {
            // TODO: Make this more permissive. What's the best way to do this
            // while still providing security for viewers?
            throw new IOException("Repository not whitelisted: " + theRepository);
        }
        
        MatchData theMatch = null;
        PersistenceManager pm = Persistence.getPersistenceManager();
        try {
            theMatch = pm.detachCopy(pm.getObjectById(MatchData.class, MatchData.getKeyFromJSON(theMatchJSON)));
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
    
    public void performCreationValidationChecks(String theJSON) throws IOException {
        try {
            JSONObject newMatchJSON = new JSONObject(theJSON);
            
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
    
    public void performUpdateValidationChecks(String oldJSON, String newJSON) throws IOException {
        try {
            JSONObject oldMatchJSON = new JSONObject(oldJSON);
            JSONObject newMatchJSON = new JSONObject(newJSON);
            
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
    
    public void performInternalConsistencyChecks(String theJSON) throws IOException {
        try {
            JSONObject theMatchJSON = new JSONObject(theJSON);
            
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
}