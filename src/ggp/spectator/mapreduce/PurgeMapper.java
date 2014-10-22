package ggp.spectator.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import external.JSON.JSONException;
import external.JSON.JSONObject;

import org.apache.hadoop.io.NullWritable;

public class PurgeMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  // Map over the datastore, identifying and purging entries which satisfy some criteria.
  public void map(Key key, Entity value, Context context) {
      try {
          String theJSON = ((Text)value.getProperty("theMatchJSON")).getValue();
          JSONObject theMatch = new JSONObject(theJSON);

          if(shouldPurge(theMatch)) {
              DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
              datastore.delete(key);
              context.getCounter("Overall", "Purged").increment(1);
          } else {
              context.getCounter("Overall", "Ignored").increment(1);
          }          
      } catch (Exception e) {
          context.getCounter("Overall", "Unreadable").increment(1);
      }
  }
  
  public static boolean shouldPurge(JSONObject theMatch) throws JSONException {
      return theMatch.has("matchHostPK") && theMatch.getString("matchHostPK") != null && theMatch.getString("matchHostPK").equals("0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAgjgpQmdHCwKOrM1KS3u4d6CwAqbr715o0ARK+bQKXH8aSXfQCPUdyjCG6KQ1CENr4VdBWS8UYvPlMcCmjfPQkFJ+u7XF7/TDuDgYMUDAC5qJ4UmMD49bzlE7nW+4dVHSUsJr2WWSMgh7vbSbvIUhpCsTxG0OxIcwZ0cY0NwnF2RVXBLVH1nsey4ExjtuyI3Jp21yKzX1CDUwhrczp69j4wVSEFzeiyfNk70SZhi14q5lxZ1O/h3ZlhnIAU5Ko7Cej9Kh6Xd6OfBriav4yBTBcVV+uPQvRsyAgRQpKe/5qVxCnVejcTpaNPXZcjH3GMB/F3IZPSVZ2uluaf1U3EPUrwIDAQAB");
  }
}