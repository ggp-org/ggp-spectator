package ggp.spectator;

import static com.google.appengine.api.taskqueue.RetryOptions.Builder.withTaskRetryLimit;
import static com.google.appengine.api.taskqueue.TaskOptions.Builder.withUrl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.servlet.http.*;

import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import org.json.JSONException;
import org.json.JSONObject;

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
        if (theURL.startsWith("/matches/feeds/")) {
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
    
    private final static int PING_RETRIES = 10;
    private static void addTaskToPingHub(String theFeedURL) {
        QueueFactory.getDefaultQueue().add(withUrl("/tasks/ping_hub").method(Method.POST).param("feedURL", theFeedURL).retryOptions(withTaskRetryLimit(PING_RETRIES)));
    }

    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");            
        
        if (req.getRequestURI().equals("/tasks/ping_hub")) {
        	int nRetryAttempt = Integer.parseInt(req.getHeader("X-AppEngine-TaskRetryCount"));
            String theFeedURL = req.getParameter("feedURL");
            try {
            	PuSHPublisher.pingHub("http://pubsubhubbub.appspot.com/", theFeedURL);
            	resp.setStatus(200);
            } catch (Exception e) {
            	resp.setStatus(503);
        		// For the first few exceptions, silently issue errors to task queue to trigger retries.
        		// After a few retries, start surfacing the exceptions, since they're clearly not transient.
            	// This reduces the amount of noise in the error logs caused by transient PuSH hub errors.
            	if (nRetryAttempt > PING_RETRIES - 3) {
            		throw new RuntimeException(e);
            	}
            }            
            return;
        }
        
        String theURL = req.getRequestURI();
        if(!theURL.equals("/"))
            return;

        try {
            JSONObject theMatchJSON;
            String theAuthToken = req.getParameter("AUTH");
            try {
                theMatchJSON = new JSONObject(req.getParameter("DATA"));
                String theRepository = new URL(theMatchJSON.getString("gameMetaURL")).getHost();
                if (!theRepository.equals("games.ggp.org")) {
                    // TODO: Make this more permissive. What's the best way to do this
                    // while still providing security for viewers?
                    throw new MatchValidation.ValidationException("Repository not whitelisted: " + theRepository);
                }
                if (!theMatchJSON.getString("gameMetaURL").contains("//games.ggp.org/base") &&
                    !theMatchJSON.getString("gameMetaURL").contains("//games.ggp.org/dresden")) {
                    // TODO: Make this more permissive. What's the best way to do this
                    // while still providing security for viewers?
                    throw new MatchValidation.ValidationException("Repository not whitelisted: " + theMatchJSON.getString("gameMetaURL"));                    
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

            // Respond to the match host with the key.
            resp.getWriter().println(theMatch.getMatchKey());
            resp.getWriter().close();
            
            // Add background tasks to ping the PuSH hubs.
            AtomKeyFeed.addRecentMatchKey("updatedFeed", theMatch.getMatchKey());            
            addTaskToPingHub("http://matches.ggp.org/matches/feeds/updatedFeed.atom");
            addTaskToPingHub("http://matches.ggp.org/matches/" + theMatch.getMatchKey() + "/feed.atom");

            // When the match is completed, update that feed and ping the PuSH hub.
            try {
                if (theMatchJSON.has("isCompleted") && theMatchJSON.getBoolean("isCompleted")) {
                    AtomKeyFeed.addRecentMatchKey("completedFeed", theMatch.getMatchKey());
                    addTaskToPingHub("http://matches.ggp.org/matches/feeds/completedFeed.atom");
                }
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
            
            // Also manually ping the database server, in case PuSH is misbehaving.
            // TODO(schreib): Add this manual ping back in if we're seeing match updates being dropped.
            // RemoteResourceLoader.loadJSON("http://database.ggp.org/ingest_match?matchURL="+URLEncoder.encode("http://matches.ggp.org/matches/" + theMatch.getMatchKey() + "/", "UTF-8"));
        } catch (MatchValidation.ValidationException ve) {
            // For now, we want to pass up any MatchValidation exceptions all the way to the top,
            // so they appear in the server logs and can be acted upon quickly.
            throw new RuntimeException(ve);            
        }
    }
    
    public void doOptions(HttpServletRequest req, HttpServletResponse resp) throws IOException {  
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");    
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
}