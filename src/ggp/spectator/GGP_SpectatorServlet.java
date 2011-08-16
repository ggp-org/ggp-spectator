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
                    throw new MatchValidation.ValidationException("Repository not whitelisted: " + theRepository);
                }
            } catch (JSONException e) {
                throw new MatchValidation.ValidationException(e.toString());
            }
    
            MatchData theMatch = null;
            PersistenceManager pm = Persistence.getPersistenceManager();
            try {
                theMatch = MatchData.loadExistingMatchFromJSON(pm, theMatchJSON);
                if (!theMatch.getAuthToken().equals(theAuthToken)) {
                    throw new MatchValidation.ValidationException("Unauthorized auth token used to update match.");
                }
                MatchValidation.performUpdateValidationChecks(theMatch.getMatchJSON(), theMatchJSON);
                MatchValidation.performInternalConsistencyChecks(theMatchJSON);
                theMatch.setMatchJSON(theMatchJSON);
                pm.makePersistent(theMatch);
            } catch(JDOObjectNotFoundException e) {
                MatchValidation.performCreationValidationChecks(theMatchJSON);
                MatchValidation.performInternalConsistencyChecks(theMatchJSON);
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
            PuSHPublisher.pingHub("http://pubsubhubbub.appspot.com/", "http://matches.ggp.org/matches/feeds/updatedFeed.atom");
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
        } catch (MatchValidation.ValidationException ve) {
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