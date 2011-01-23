var ResourceLoader = {

  // This function can load two types of rulesheets: KIF and HRF.
  // These formats are interchangeable in a straightforward way,
  // and given a URL for either it will load the rulesheet and
  // automatically generate its counterpart in the other format.
  // 
  // It will return an array with two elements: the HRF rules,
  // readable by the JS GGP libraries, and the KIF rules, which are
  // the standard format for describing games.
  //
  // The HRF rules should be used for instantiating the state machine.
  // The KIF rules can be displayed to the end user, if desired.
  load_rulesheet: function (url) {
    if (url.length < 3) { return [] };
    var type = url.slice(url.length-3).toLowerCase();
    if (type == 'kif') { return this.internals.importkif(url) };
    if (type == 'hrf') { return this.internals.importhrf(url) };
    return []
  },

  load_xml: function (url) {
    return this.internals.load_data(url)[1];
  },

  load_js: function (url) {
      return eval('('+this.internals.load_data(url)[0]+')');
  },
  
  load_json: function (url) {
      return JSON.parse(this.internals.load_data(url)[0]);
  },    
  
  load_raw: function (url) {
    return this.internals.load_data(url)[0];
  },
  
  // For stylesheets, we load both the raw text, and the XML document.
  load_stylesheet: function (url) {
    return this.internals.load_data(url);
  },  
  
  internals : {
    load_data: function (url) {
      var xhttp = new XMLHttpRequest();      
      xhttp.open("GET", url, false);
      xhttp.send("");
      if (xhttp.responseText) {
        return [xhttp.responseText, xhttp.responseXML]
      }
      alert('Cannot load: ' + url);
      return false
    }, 
  
    importkif: function (url) {
      var rulesheet = this.load_data(url)[0];
      var rules = readkifdata(rulesheet);

      var right_rules = grindem(rules);
      var print_rules = printem(rules);
      
      return [right_rules, print_rules];
    },

    importhrf: function (url) {
      var rulesheet = this.load_data(url)[0];

      var right_rules = rulesheet;
      var print_rules = printem(readdata(rulesheet));
      
      return [right_rules, print_rules];
    }
  }
}