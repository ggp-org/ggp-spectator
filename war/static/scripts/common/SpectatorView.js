if (typeof Object.create !== 'function') {
  Object.create = function (o) {
    var F = function () {}
    F.prototype = o;
    return new F();
  }
}

// This allows you to construct a SpectatorView on a web page.
// This requires the following includes in the HEAD of the page:
//   * Channel API: "/_ah/channel/jsapi"
//   * jQuery Library: "//ajax.googleapis.com/ajax/libs/jquery/1.4.4/jquery.min.js"
//
// This also requires the acquisition of a channel token, which
// is stored in the global variable "theChannelToken". This is the
// only thing preventing you from instantiating multiple SpectatorViews
// on a single web page; hopefully that will be fixed at some point.
var SpectatorView = {
  gameVizDiv: null,
  gameOldVizDiv: null,
  
  rendering: false,
  pending_render: false,
  
  getHeight: function () { return (window.innerHeight - 125) * 0.95; },
  getWidth: function () { return (window.innerWidth - 125) * 0.95; },
  
  state: null,  
  matchData: null,
  stylesheet: null,  

  // Render the current state of the match, using a
  // smooth transition between old and new states.
  render: function() {
    if (this.rendering) {
      this.pending_render = true;
      return;
    }
    
    var height = this.getHeight();
    var width = this.getWidth();    
    if (height < 100) height = 100;
    if (width < 100) width = 100;    
    if (width < height) height = width;
    if (height < width) width = height;
      
    this.gameOldVizDiv.innerHTML = this.gameVizDiv.innerHTML;
    this.gameOldVizDiv.style.cssText = '';

    this.gameVizDiv.innerHTML = '';
    StateRenderer.render_state_using_xslt(this.state, this.stylesheet, this.gameVizDiv, width, height);

    var thisRef = this;
    this.rendering = true;    
    $(spectatorView.gameOldVizDiv).fadeOut('slow', function() {
      thisRef.rendering = false;
      if (thisRef.pending_render) {
        thisRef.pending_render = false;
        thisRef.render();
      }
    });
  },
  
  // Given a match data object, return the current state, which is at the end
  // of the ordered list of states in the match.
  getStateFromMatchData: function(m) {
    var z = m.states;
    return SymbolList.symbolListIntoArray(z[z.length-1]);
  },
  
  initialize: function (match_url, spectator_div) {
    // First, create the HTML structure for holding the spectator view.
    // This consists of a DIV for the game visualization (gameVizDiv) and
    // a DIV that holds the previous state of the game, which is needed for
    // smooth transitions between states.
    var topDiv = document.createElement('div');
    topDiv.style.cssText = "position: relative;";
    var firstSubDiv = document.createElement('div');
    var secondSubDiv = document.createElement('div');
    firstSubDiv.style.cssText = "position: absolute; top:0; left:0; width: 100%; z-index:1;";
    secondSubDiv.style.cssText = "position: absolute; top:0; left:0; width: 100%; z-index:2;";
    this.gameVizDiv = document.createElement('div');
    this.gameOldVizDiv = document.createElement('div');   
    
    firstSubDiv.appendChild(this.gameVizDiv);
    secondSubDiv.appendChild(this.gameOldVizDiv);
    topDiv.appendChild(firstSubDiv);
    topDiv.appendChild(secondSubDiv);
    spectator_div.appendChild(topDiv);
    
    // Next, load the resources for the match. We need to load the current state,
    // and also the stylesheet associated with the game being played. First, load
    // the current state of the game.
    var matchString = ResourceLoader.load_raw(match_url);
    this.matchData = JSON.parse(matchString);        
    this.state = this.getStateFromMatchData(this.matchData);
    
    // Next, load the stylesheet associated with the game being played. This involves
    // calling out to the repository server that hosts the game being played.
    var gameURL = this.matchData.gameMetaURL;
    var metadata = ResourceLoader.load_json(gameURL);      
    this.stylesheet = ResourceLoader.load_stylesheet(gameURL + metadata.stylesheet);      

    // If we need to load the description, do that as well.
    // TODO(schreib): Set the description in a more elegant way.
    if ("description" in metadata && document.getElementById('desc_div') != null) {
      var description = ResourceLoader.load_raw(gameURL + metadata.description);
      var desc_div = document.getElementById('desc_div');
      desc_div.innerHTML = '<b>Game Description:</b> ' + description;       
    }
    
    // TODO(schreib): Set the name_div in a more elegant way.
    var name_div = document.getElementById('name_div');
    name_div.innerHTML = 'Showing match: ' + this.matchData.matchId;

    // Create a callback that will update the match state based on
    // messages from the browser channel.
    var thisRef = this;
    function update_state_via_channel(channelMessage) {
      var newMatchData = JSON.parse(channelMessage.data);      
      thisRef.state = thisRef.getStateFromMatchData(newMatchData);
      thisRef.matchData = newMatchData;
      thisRef.render();

      if (newMatchData.isCompleted) {
        // TODO(schreib): Better reflect that the match is over.
        var name_div = document.getElementById('name_div');
        name_div.innerHTML += ' ... GAME OVER'
        return;
      }
    }
    
    // Open a Browser Channel to the Spectator Server.
    // We will receive updates to the match state over this channel.
    var channel = new goog.appengine.Channel(theChannelToken);
    channel.open().onmessage = update_state_via_channel;   
  },
  
  // Constructor, to make new SpectatorViews.
  construct: function (match_url, spectator_div) {
    var my_spectator_view = Object.create(SpectatorView);
    my_spectator_view.initialize(match_url, spectator_div);
    return my_spectator_view;
  }
}