var StateRenderer = {
  xmlize_state: function (state) {
    var node = document.createElement('state');
    for (var i=0; i<state.length; i++) {
      var fact = document.createElement('fact');
      var relation = document.createElement('relation');
      relation.innerHTML = state[i][0];
      fact.appendChild(relation);
      for (var j=1; j<state[i].length; j++) {
        var argument = document.createElement('argument');
        argument.innerHTML = state[i][j];
        fact.appendChild(argument);
      }
      node.appendChild(fact);
    }
    return node;
  },

  render_state_using_xslt: function (state, xsl_both, vdiv, width, height) {
    var xsl = xsl_both[1];
    var xsl_raw = xsl_both[0];
  
    var xml_node = this.xmlize_state(state);
    var output = null; 
  
    if (navigator.userAgent.toLowerCase().search("android") > -1) {
        // Android's browser doesn't have a built-in XSLT processor,
        // but aside from that it's perfectly capable of doing GGP,
        // so we have it fall back to a JS-based XSLT processor.
    
	    var androidTab = document.createElement('xslt_note');
	    androidTab.innerHTML = "<b>Using Javascript XSLT Processor!</b>";
	    vdiv.appendChild(androidTab);
	    
	    output = run_AJAXSLT_Processor(xml_node, xsl_raw);
	} else {
	    var xsltProcessor = new XSLTProcessor();    
	    xsltProcessor.importStylesheet(xsl);
	
	    xsltProcessor.setParameter(null, "width", ""+width);
	    xsltProcessor.setParameter(null, "height", ""+height);    
	            
	    output = xsltProcessor.transformToFragment(xml_node, document);
	}
	
    if (!output) {
      vdiv.innerHTML = "<table border='5' width='" + width + "' height='" + height + "'><tr><td><center><b>[ERROR: Could not render via XSLT Processor]</b></center></td></tr></table>";
    } else {      
      vdiv.appendChild(output);
    }
  }
}