package ggp.spectator.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;

import util.crypto.SignableJSON;

public class AdjustmentMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  // Map over the datastore, identifying and modifying entries which satisfy some criteria.
  public void map(Key key, Entity value, Context context) {
      try {
          String theJSON = ((Text)value.getProperty("theMatchJSON")).getValue();
          JSONObject theMatch = new JSONObject(theJSON);

          if(shouldAdjust(theMatch)) {
              try {
                  doSignedAdjustment(theMatch);
                  DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
                  value.setProperty("theMatchJSON", new Text(theMatch.toString()));
                  datastore.put(value);
                  context.getCounter("Overall", "Adjusted").increment(1);
              } catch (Exception e) {
                  context.getCounter("Overall", "Failure").increment(1);
              }
          } else {
              context.getCounter("Overall", "Ignored").increment(1);
          }          
      } catch (Exception e) {
          context.getCounter("Overall", "Unreadable").increment(1);
      }      
  }
  
  public static boolean shouldAdjust(JSONObject theMatchJSON) throws JSONException {
      // TODO: Fill this in!
      return false;
  }
  
  public static void doSignedAdjustment(JSONObject theMatchJSON) throws JSONException {
      doAdjustment(theMatchJSON);
      
      // Once we've made the adjustment, we need to re-sign the match. Note this can only be done
      // for matches for which we have the host's private signing keys.
      if (theMatchJSON.has("matchHostPK")) {
          String thePK = theMatchJSON.getString("matchHostPK");
          String theSK = getSK(thePK);
          theMatchJSON.remove("matchHostPK");
          theMatchJSON.remove("matchHostSignature");
          SignableJSON.signJSON(theMatchJSON, thePK, theSK);
          SignableJSON.verifySignedJSON(theMatchJSON);
      }
  }
  
  public static void doAdjustment(JSONObject theMatchJSON) throws JSONException {      
      // TODO: Fill this in!      
  }
  
  public static String getSK(String thePK) {
      // TODO: Fill this in!      
      return null;
  }
}