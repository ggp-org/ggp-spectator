var SymbolList = {
    arrayIntoSymbolList: function(z) {
        if(!this.isArray(z))
            return z;
        
        var s = "( ";
        for(var i = 0; i < z.length; i++) {
            s += this.arrayIntoSymbolList(z[i]) + " ";
        };
        s += ")";
        return s;
    },

    symbolListIntoArray: function(symbolList) {
        // Clean up the string representation of the list
        symbolList = symbolList.replace(/\(/g, " ( ");
        symbolList = symbolList.replace(/\)/g, " ) ");
        symbolList = symbolList.replace(/\s+/g, " ");
        
        // Trim starting/ending whitespace from the string
        symbolList = symbolList.replace(/^\s+|\s+$/g,"");
        
        // Tokenize and convert into a nested array.
        var tokens = symbolList.split(" ");
        return this.convert(tokens);
    },
    
    // ========= INTERNAL IMPLEMENTATION ============
    
    convert: function(tokens) {
        if(tokens.length == 0) return [];
        if(tokens[0] == "(") return this.convertList(tokens);
        return tokens.splice(0,1)[0];
    },
    
    convertList: function(tokens) {
        var contents = []
        tokens.splice(0,1);
        while (tokens[0] != ")") {
            contents.push(this.convert(tokens));
        }
        tokens.splice(0,1);
        return contents;
    },

    isArray: function (array) {
      return !(
        !array || 
        (!array.length || array.length == 0) || 
        typeof array !== 'object' || 
        !array.constructor || 
        array.nodeType || 
        array.item 
      );
    }
}