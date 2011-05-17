package ggp.spectator;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.repackaged.org.json.JSONObject;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;

import java.util.logging.Logger;

public class SimpleMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  private static final Logger log = Logger.getLogger(SimpleMapper.class.getName());

  public SimpleMapper() {
      ;
  }

  @Override
  public void taskSetup(Context context) {
    log.warning("Doing per-task setup");
  }

  @Override
  public void taskCleanup(Context context) {
    log.warning("Doing per-task cleanup");
  }

  @Override
  public void setup(Context context) {
    log.warning("Doing per-worker setup");
  }

  @Override
  public void cleanup(Context context) {
    log.warning("Doing per-worker cleanup");    
  }
  
  public static void recordWhetherJSONHas(Context context, JSONObject theMatch, String theKey) {
      if (theMatch.has(theKey)) context.getCounter("hasField", theKey).increment(1);
      else context.getCounter("lacksField", theKey).increment(1);
  }

  // Quick observation map over the datastore, to collect numbers
  // on how often various match descriptor fields are being used.
  @Override
  public void map(Key key, Entity value, Context context) {
    log.warning("Mapping key: " + key);

    try {
        String theJSON = ((Text)value.getProperty("theMatchJSON")).getValue();
        JSONObject theMatch = new JSONObject(theJSON);

        int nClientIDs = 0;
        try {
            MatchData m = MatchData.loadExistingMatchFromJSON(Persistence.getPersistenceManager(), theMatch);
            nClientIDs = m.numClientIDs();
        } catch (Exception q) {
            context.getCounter("clientIDs", "Unparseable").increment(1);
        }
        
        recordWhetherJSONHas(context, theMatch, "matchId");
        recordWhetherJSONHas(context, theMatch, "startTime");
        recordWhetherJSONHas(context, theMatch, "randomToken");
        recordWhetherJSONHas(context, theMatch, "matchHostPK");
        recordWhetherJSONHas(context, theMatch, "startClock");
        recordWhetherJSONHas(context, theMatch, "playClock");
        recordWhetherJSONHas(context, theMatch, "states");
        recordWhetherJSONHas(context, theMatch, "moves");
        recordWhetherJSONHas(context, theMatch, "errors");
        recordWhetherJSONHas(context, theMatch, "stateTimes");
        recordWhetherJSONHas(context, theMatch, "isCompleted");
        recordWhetherJSONHas(context, theMatch, "goalValues");
        recordWhetherJSONHas(context, theMatch, "gameMetaURL");
        recordWhetherJSONHas(context, theMatch, "gameRoleNames");
        recordWhetherJSONHas(context, theMatch, "gameName");
        recordWhetherJSONHas(context, theMatch, "gameRulesheetHash");
        recordWhetherJSONHas(context, theMatch, "playerIds");
        recordWhetherJSONHas(context, theMatch, "playerPresenceSignatures");
        recordWhetherJSONHas(context, theMatch, "playerStateSignatures");
        recordWhetherJSONHas(context, theMatch, "playerSignatures");
        recordWhetherJSONHas(context, theMatch, "matchHostSignature");
        
        try {
            boolean isCompleted = theMatch.getBoolean("isCompleted");
            if (isCompleted) {
                context.getCounter("isCompleted", "Yes").increment(1);
                context.getCounter("clientIDs", "completed").increment(nClientIDs);
            } else {
                context.getCounter("isCompleted", "No").increment(1);
                context.getCounter("clientIDs", "ongoing").increment(nClientIDs);
            }
        } catch (Exception e2) {
            context.getCounter("isCompleted", "Unknown").increment(1);
            context.getCounter("clientIDs", "Unknown").increment(nClientIDs);
        }
        
        context.getCounter("Overall", "Readable").increment(1);
    } catch (Exception e) {
        context.getCounter("Overall", "Unreadable").increment(1);
    }
  }
}