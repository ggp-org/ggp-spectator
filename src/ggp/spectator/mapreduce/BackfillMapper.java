package ggp.spectator.mapreduce;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;
import org.ggp.galaxy.shared.loader.RemoteResourceLoader;

import java.util.logging.Logger;

public class BackfillMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  private static final Logger log = Logger.getLogger(BackfillMapper.class.getName());

  public BackfillMapper() {
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

  // Quick observation map over the datastore, to collect numbers
  // on how often various match descriptor fields are being used.
  @Override
  public void map(Key key, Entity value, Context context) {
    log.warning("Mapping key: " + key);

    try {
        String forgedAtomText = "<link href=\"http://matches.ggp.org/matches/" + key.getName() + "/\"/>";
        RemoteResourceLoader.postRawWithTimeout("http://stats.ggp.org/ingestion/", forgedAtomText, 10000);
        context.getCounter("Overall", "Sent").increment(1);
    } catch (Exception e) {
        context.getCounter("Overall", "Failed").increment(1);
    }
  }
}