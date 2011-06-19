package ggp.spectator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import org.datanucleus.store.query.AbstractQueryResult;

@PersistenceCapable
public class RecentMatchKeys {
    @SuppressWarnings("unused")
    @PrimaryKey @Persistent private String thePrimaryKey;
    @Persistent private List<String> recentMatchKeys;
    private static final int kRecentMatchKeysToRecord = 100;    
    
    private RecentMatchKeys() {
        thePrimaryKey = "RecentMatchKeys";
        recentMatchKeys = new ArrayList<String>();
    }
    
    /* Static accessor methods */
    public static void addRecentMatchKey(String theKey) {
        Set<RecentMatchKeys> theSet = new HashSet<RecentMatchKeys>();
        PersistenceManager pm = Persistence.getPersistenceManager();
        try {
            Iterator<?> sqr = ((AbstractQueryResult) pm.newQuery(RecentMatchKeys.class).execute()).iterator();
            while (sqr.hasNext()) {
                theSet.add((RecentMatchKeys)sqr.next());
            }            
        } catch(JDOObjectNotFoundException e) {
            ;
        }
        RecentMatchKeys recent;
        if (!theSet.isEmpty()) {
            recent = theSet.iterator().next();
        } else {
            recent = new RecentMatchKeys();
        }
        recent.recentMatchKeys.add(theKey);
        if (recent.recentMatchKeys.size() > kRecentMatchKeysToRecord) {
            recent.recentMatchKeys.remove(0);
        }
        pm.makePersistent(recent);
        pm.close();
    }
    
    public static List<String> getRecentMatchKeys() {
        Set<RecentMatchKeys> theStates = Persistence.loadAll(RecentMatchKeys.class);
        if (theStates.size() > 0) {
            return theStates.iterator().next().recentMatchKeys;
        } else {
            return new RecentMatchKeys().recentMatchKeys;
        }        
    }
}