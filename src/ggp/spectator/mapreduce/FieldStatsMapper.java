package ggp.spectator.mapreduce;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import org.json.JSONObject;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;

import ggp.spectator.MatchData;

public class FieldStatsMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  public static void recordWhetherJSONHas(Context context, JSONObject theMatch, String theKey) {
      if (theMatch.has(theKey)) context.getCounter("hasField", theKey).increment(1);
      else context.getCounter("lacksField", theKey).increment(1);
  }

  // Quick observation map over the datastore, to collect numbers
  // on how often various match descriptor fields are being used.
  @Override
  public void map(Key key, Entity value, Context context) {
    try {
        String theJSON = ((Text)value.getProperty("theMatchJSON")).getValue();
        JSONObject theMatch = new JSONObject(theJSON);

        int nClientIDs = 0;
        try {            
            MatchData m = MatchData.loadMatchData(key.getName());
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
                context.getCounter("clientIDs", "forCompletedMatches").increment(nClientIDs);
            } else {
                context.getCounter("isCompleted", "No").increment(1);
                context.getCounter("clientIDs", "forOngoingMatches").increment(nClientIDs);
            }
        } catch (Exception e2) {
            context.getCounter("isCompleted", "forIndeterminateMatches").increment(1);
            context.getCounter("clientIDs", "Unknown").increment(nClientIDs);
        }
        
        context.getCounter("Overall", "Readable").increment(1);
    } catch (Exception e) {
        context.getCounter("Overall", "Unreadable").increment(1);
    }
  }
}