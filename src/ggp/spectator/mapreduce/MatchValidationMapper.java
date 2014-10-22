package ggp.spectator.mapreduce;

import external.JSON.JSONObject;
import ggp.spectator.MatchValidation;
import ggp.spectator.MatchValidation.ValidationException;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;

public class MatchValidationMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  // Map over the datastore, identifying matches that don't pass validation.
  public void map(Key key, Entity value, Context context) {
      try {
          String theJSON = ((Text)value.getProperty("theMatchJSON")).getValue();
          JSONObject theMatch = new JSONObject(theJSON);
          String bucket = theMatch.has("matchHostPK") ? "Signed" : "Anonymous";

          try {
              MatchValidation.performInternalConsistencyChecks(theMatch);
              context.getCounter(bucket, "Passed Validation").increment(1);
              context.getCounter("Overall", "Passed Validation").increment(1);
          } catch (ValidationException e) {
              String theValidationError = "Failure [" + e.getStackTrace()[0].getFileName() + ":" + e.getStackTrace()[0].getLineNumber() + "] ";
              context.getCounter(bucket, "Failed Validation").increment(1);;
              context.getCounter(bucket, theValidationError).increment(1);
              context.getCounter("Overall", "Failed Validation").increment(1);;
          }          
      } catch (Exception e) {
          context.getCounter("Overall", "Unreadable").increment(1);
      }      
  }
}