package ggp.spectator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.servlet.http.*;

import com.google.appengine.api.channel.ChannelServiceFactory;

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
        if(theURL.endsWith("/channel.js")) {
            theURL = theURL.substring(0, theURL.length() - 11);
            writeChannelLibrary(resp, theURL);
            return;
        }
        if(theURL.endsWith("/viz.html")) {
            writeStaticPage(resp, "MatchPage.html");
            return;
        }
        if(theURL.endsWith("/viz2.html")) {
            writeStaticPage(resp, "MatchPage2.html");
            return;
        }
        if(theURL.length() == 0) {
            Set<MatchData> theMatches = MatchData.loadMatches();
            List<String> theMatchKeys = new ArrayList<String>();
            for(MatchData m : theMatches) theMatchKeys.add(m.getMatchKey());
            resp.getWriter().println(MatchData.renderArrayAsJSON(theMatchKeys, true));
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
    
    public void writeChannelLibrary(HttpServletResponse resp, String theKey) throws IOException {        
        MatchData theMatch = null;
        PersistenceManager pm = Persistence.getPersistenceManager();
        try {
            theMatch = pm.detachCopy(pm.getObjectById(MatchData.class, theKey));
            
            String theClientID = MatchData.getRandomString(32);                
            String theToken = ChannelServiceFactory.getChannelService().createChannel(theClientID);
            theMatch.addClientID(theClientID);
            
            resp.getWriter().println("theChannelID = \"" + theClientID + "\";\n");
            resp.getWriter().println("theChannelToken = \"" + theToken + "\";\n");
            
            pm.makePersistent(theMatch);
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
            theMatch.setMatchJSON(theMatchJSON);
            pm.makePersistent(theMatch);
        } catch(JDOObjectNotFoundException e) {
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
}