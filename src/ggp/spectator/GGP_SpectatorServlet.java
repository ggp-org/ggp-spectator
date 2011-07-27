package ggp.spectator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.servlet.http.*;

import util.crypto.SignableJSON;
import util.symbol.factory.SymbolFactory;
import util.symbol.factory.exceptions.SymbolFormatException;
import util.symbol.grammar.SymbolList;

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
        if (theURL.startsWith("/data/")) {
            handleRPC(resp, theURL.substring("/data/".length()));
            return;
        } else if (theURL.startsWith("/matches/feeds/")) {
            handleFeed(resp, theURL.substring("/matches/feeds/".length()));
            return;
        } else if(!theURL.startsWith("/matches/")) {
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
        if(theURL.trim().length() == 0) {
            // Currently no content at "/matches/" right now.
            resp.setStatus(404);
            return;
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

        try {
            JSONObject theMatchJSON;
            String theAuthToken = req.getParameter("AUTH");
            try {
                theMatchJSON = new JSONObject(req.getParameter("DATA"));
                String theRepository = new URL(theMatchJSON.getString("gameMetaURL")).getHost();
                if (!theRepository.equals("ggp-repository.appspot.com") &&
                    !theRepository.equals("games.ggp.org")) {
                    // TODO: Make this more permissive. What's the best way to do this
                    // while still providing security for viewers?
                    throw new ValidationException("Repository not whitelisted: " + theRepository);
                }
            } catch (JSONException e) {
                throw new ValidationException(e.toString());
            }
    
            MatchData theMatch = null;
            PersistenceManager pm = Persistence.getPersistenceManager();
            try {
                theMatch = MatchData.loadExistingMatchFromJSON(pm, theMatchJSON);
                if (!theMatch.getAuthToken().equals(theAuthToken)) {
                    throw new ValidationException("Unauthorized auth token used to update match.");
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
            AtomKeyFeed.addRecentMatchKey("updatedFeed", theMatch.getMatchKey());            
            PuSHPublisher.pingHub("http://pubsubhubbub.appspot.com/", "http://matches.ggp.org/matches/" + theMatch.getMatchKey() + "/feed.atom");
            
            // When the match is completed, update that feed and ping the PuSH hub.
            try {
                if (theMatchJSON.has("isCompleted") && theMatchJSON.getBoolean("isCompleted")) {
                    AtomKeyFeed.addRecentMatchKey("completedFeed", theMatch.getMatchKey());
                    PuSHPublisher.pingHub("http://pubsubhubbub.appspot.com/", "http://matches.ggp.org/matches/feeds/completedFeed.atom");
                }
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        } catch (ValidationException ve) {
            resp.setStatus(500);
            resp.getWriter().println(ve.toString());
            resp.getWriter().close();
        }
    }
    
    public void doOptions(HttpServletRequest req, HttpServletResponse resp) throws IOException {  
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");    
    }
    
    private void handleRPC(HttpServletResponse resp, String theURL) throws IOException {
        JSONObject theResult = null;
        // TODO(schreib): ... fill in "theResult" based on "theURL" ...
        if (theResult == null) {
            resp.setStatus(404);
        } else {
            resp.setStatus(200);
            resp.getWriter().println(theResult.toString());
        }
    }
    
    private void handleFeed(HttpServletResponse resp, String theFeedKey) throws IOException {
        if (theFeedKey.isEmpty()) {
            resp.setStatus(400);
            resp.getWriter().close();
            return;
        }
        String theFeed = null;
        if (theFeedKey.endsWith(".atom")) {
            theFeed = AtomKeyFeed.getAtomFeed(theFeedKey.replace(".atom", ""));
            resp.setContentType("application/atom+xml");
        } else if (theFeedKey.endsWith(".json")) {
            theFeed = AtomKeyFeed.getJsonFeed(theFeedKey.replace(".json", ""));
            resp.setContentType("text/javascript");
        }
        if (theFeed == null) {
            resp.setStatus(404);
        } else {
            resp.setStatus(200);
            resp.getWriter().println(theFeed);            
        }
        resp.getWriter().close();
    }
    
    // =======================================================================
    // Validation checks, to make sure bad information is not published to the
    // spectator server by a malicious adversary or malfunctioning system. These
    // are not comprehensive, but are intended to provide basic sanity guarantees
    // so that we aren't storing total nonsense in the spectator server.
    
    class ValidationException extends IOException {
        public ValidationException(String x) {
            super(x);
        }
    }
    
    public void performCreationValidationChecks(JSONObject newMatchJSON) throws ValidationException {
        try {
            verifyReasonableTime(newMatchJSON.getLong("startTime"));

            if (newMatchJSON.getString("randomToken").length() < 12) {
                throw new ValidationException("Random token is too short.");
            }

            if (newMatchJSON.has("matchHostPK") && !newMatchJSON.has("matchHostSignature")) {
                throw new ValidationException("Signatures required for matches identified with public keys.");
            }
        } catch(JSONException e) {
            throw new ValidationException("Could not parse JSON: " + e.toString());
        }
    }
    
    public void performUpdateValidationChecks(JSONObject oldMatchJSON, JSONObject newMatchJSON) throws ValidationException {
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
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "gameRoleNames", false, false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "playerNamesFromHost", false, false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "moves", true, false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "errors", true, false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "states", true, true);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "stateTimes", true, false);            

            if (oldMatchJSON.has("isCompleted") && newMatchJSON.has("isCompleted") && oldMatchJSON.getBoolean("isCompleted") && !newMatchJSON.getBoolean("isCompleted")) {
                throw new ValidationException("Cannot transition from completed to not-completed.");
            }            
        } catch(JSONException e) {
            throw new ValidationException("Could not parse JSON: " + e.toString());
        }
    }
    
    public void performInternalConsistencyChecks(JSONObject theMatchJSON) throws ValidationException {
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
            
            try {
                String theGameURL = theMatchJSON.getString("gameMetaURL");
                String theSuffix = theGameURL.substring(theGameURL.lastIndexOf("/v"));                
                Integer.parseInt(theSuffix.substring(2, theSuffix.length()-1));
            } catch (NumberFormatException nfe) {
                throw new ValidationException("gameMetaURL is not properly version-qualified.");
            } catch (IndexOutOfBoundsException ibe) {
                throw new ValidationException("gameMetaURL is not properly version-qualified.");
            }

            int movesLength = theMatchJSON.getJSONArray("moves").length();
            int statesLength = theMatchJSON.getJSONArray("states").length();
            int stateTimesLength = theMatchJSON.getJSONArray("stateTimes").length();
            if (statesLength != stateTimesLength) {
                throw new ValidationException("There are " + statesLength + " states, but " + stateTimesLength + " state times. Inconsistent!");
            }
            if (statesLength != movesLength+1) {
                throw new ValidationException("There are " + statesLength + " states, but " + movesLength + " moves. Inconsistent!");
            }

            long theTime = theMatchJSON.getLong("startTime");
            verifyReasonableTime(theMatchJSON.getLong("startTime"));
            for (int i = 0; i < stateTimesLength; i++) {
                verifyReasonableTime(theMatchJSON.getJSONArray("stateTimes").getLong(i));
                if (theTime > theMatchJSON.getJSONArray("stateTimes").getLong(i)) {
                    throw new ValidationException("Time sequence goes backward!");
                } else {
                    theTime = theMatchJSON.getJSONArray("stateTimes").getLong(i);
                }
            }
            
            if (theMatchJSON.has("matchHostPK")) {
                if (!SignableJSON.isSignedJSON(theMatchJSON)) {
                    throw new ValidationException("Match has a host-PK but is not signed!");
                }
                if (!SignableJSON.verifySignedJSON(theMatchJSON)) {
                    throw new ValidationException("Match has a host-PK and is signed, but signature does not validate!");
                }
            } else {
                if (theMatchJSON.has("matchHostSignature")) {
                    throw new ValidationException("Any match with a signature must also contain a matchHostPK field.");
                }
            }
        } catch(JSONException e) {
            throw new ValidationException("Could not parse JSON: " + e.toString());
        }
    }
    
    public void verifyReasonableTime(long theTime) throws ValidationException {
        if (theTime < 0) throw new ValidationException("Time is negative!");
        if (theTime < 1200000000000L) throw new ValidationException("Time is before GGP Galaxy began.");
        if (theTime > System.currentTimeMillis() + 604800000L) throw new ValidationException("Time is after a week from now.");        
    }
    
    public void verifyHas(JSONObject obj, String k) throws ValidationException {
        if (!obj.has(k)) {
            throw new ValidationException("Could not find required field " + k);
        }
    }
    
    public void verifyEquals(JSONObject old, JSONObject newer, String k) throws JSONException, ValidationException {
        if (!old.has(k) && !newer.has(k)) {
            return;
        } else if (!old.has(k)) {
            throw new ValidationException("Incompability for " + k + ": old has null, new has [" + newer.get(k) + "].");
        } else if (!old.get(k).equals(newer.get(k))) {
            throw new ValidationException("Incompability for " + k + ": old has [" + old.get(k) + "], new has [" + newer.get(k) + "].");
        }
    }
    
    public void verifyOptionalArraysEqual(JSONObject old, JSONObject newer, String arr, boolean arrayCanExpand, boolean compareElementsAsSymbolSets) throws JSONException, ValidationException {
        if (!old.has(arr) && !newer.has(arr)) return;
        if (old.has(arr) && !newer.has(arr)) throw new ValidationException("Array " + arr + " missing from new, present in old.");
        if (!old.has(arr) && newer.has(arr)) return; // okay for the array to appear mid-way through the game
        JSONArray oldArr = old.getJSONArray(arr);
        JSONArray newArr = newer.getJSONArray(arr);
        if (!arrayCanExpand && oldArr.length() != newArr.length()) throw new ValidationException("Array " + arr + " has length " + newArr.length() + " in new, length " + oldArr.length() + " in old.");
        if (newArr.length() < oldArr.length()) throw new ValidationException("Array " + arr + " shrank from length " + oldArr.length() + " to length " + newArr.length() + ".");
        for (int i = 0; i < oldArr.length(); i++) {
            if (compareElementsAsSymbolSets) {
                Set<String> oldArrElemSet = verifyOptionalArraysEqual_GenerateSymbolSet(oldArr.get(i).toString());
                Set<String> newArrElemSet = verifyOptionalArraysEqual_GenerateSymbolSet(newArr.get(i).toString());
                if (oldArrElemSet == null) {
                    throw new ValidationException("Cannot parse symbol set in old array " + arr + " at element " + i + ".");
                } else if (newArrElemSet == null) {
                    throw new ValidationException("Cannot parse symbol set in new array " + arr + " at element " + i + ".");
                } else if (!oldArrElemSet.equals(newArrElemSet)) {
                    throw new ValidationException("Array " + arr + " has set-wise disagreement between new [" + newArr.get(i) + "] and old [" + oldArr.get(i) + "] at element " + i + ".");
                }
            } else {
                if (oldArr.get(i) instanceof JSONArray) {
                    if (!(newArr.get(i) instanceof JSONArray))
                        throw new ValidationException("Array " + arr + " used to have interior arrays but no longer does, in position " + i + ".");
                    if(!oldArr.getJSONArray(i).toString().equals(newArr.getJSONArray(i).toString()))
                        throw new ValidationException("Array " + arr + " has internal disagreement between new [" + newArr.get(i) + "] and old [" + oldArr.get(i) + "] at element " + i + ".");
                } else if (!oldArr.get(i).equals(newArr.get(i))) {
                    throw new ValidationException("Array " + arr + " has disagreement between new [" + newArr.get(i) + "] and old [" + oldArr.get(i) + "] at element " + i + ".");
                }
            }
        }
    }    
    public static Set<String> verifyOptionalArraysEqual_GenerateSymbolSet(String x) {        
        try {
            Set<String> y = new HashSet<String>();
            SymbolList l = (SymbolList)SymbolFactory.create(x);
            for (int i = 0; i < l.size(); i++) y.add(l.get(i).toString());
            return y;
        } catch (SymbolFormatException q) {
            return null;
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