if (typeof Object.create !== 'function') {
  Object.create = function (o) {
    var F = function () {}
    F.prototype = o;
    return new F();
  }
}

indexing = false;
ruleindexing = true;
var StateMachine = {
  library: [],
  
  // Convenient utility functions

  depth_charge: function () {
    var state = this.get_initial_state();
    while (!this.is_terminal(state)) {
      state = this.get_next_state(state, this.get_random_joint_moves(state) );
    }
    return state;
  },
  
  average_scores: function (rounds) {
    var roles = this.get_roles();
    sums = [];
    for(var r = 0; r < roles.length; r++) {
      sums.push(0);
    }
    for(var i = 0; i < rounds; i++) {
      final_state = this.depth_charge();
      for(var r = 0; r < roles.length; r++) {
        sums[r] += (+this.get_goal(final_state, roles[r]));
      }
    }
    avgs = [];
    for(var r = 0; r < roles.length; r++) {
      avgs.push(sums[r] / rounds);
    }
    return avgs;
  },

  run_timing_analysis: function(rounds) {
    a = new Date()
    avgs = this.average_scores(rounds)
    b = new Date()
    return [b-a, avgs]
  },
  
  // Essential functions
  
  initialize: function (rules) {
    this.library = definemorerules(seq(), readdata(rules));
  },

  get_initial_state: function () {
    var state = this.internals.findinits(this.library);
    return state
  },
  
  get_legal_moves: function (state, role) {
    return this.internals.findlegals(role, this.internals.truify(state), this.library);  
  },

  get_random_joint_moves: function (state) {
    var moves = seq()
    var roles = this.internals.findroles(this.library)
    for (var i=0; i<roles.length; i++) {
      var legals = this.internals.findlegals(roles[i], this.internals.truify(state), this.library);  
      moves.push(legals[this.internals.random_index(legals.length)])
    }
    return moves
  },

  get_next_state: function (state, moves) {
    var roles = this.internals.findroles(this.library)
    var facts = this.internals.doesify(roles,moves).concat(this.internals.truify(state));
    return this.internals.findnexts(facts, this.library);
  },
  
  get_random_next_state: function (state) {
    return this.get_next_state(state, this.get_random_joint_moves(state));
  },

  is_terminal: function (state) {
    return this.internals.findterminal(this.internals.truify(state), this.library)
  },

  get_goal: function (state, role) {
    return this.internals.findreward(role, this.internals.truify(state), this.library)
  },

  get_roles: function () {
    return this.internals.findroles(this.library)
  },
  
  // Internal functions: only useful inside of this class.
  
  internals: {
    random_index: function (n) {
      return Math.floor(Math.random()*n);
    },
  
    doesify: function (roles, actions) {
      var exp = seq();
      for (var i=0; i<roles.length; i++) {
        exp[i] = seq('does', roles[i], actions[i]);
      }
      return exp;
    },

    truify: function (state) {
      var exp = seq();
      for (var i=0; i<state.length; i++) {
        exp[i] = seq('true', state[i]);
      };
      return exp;
    },

    findroles: function (rules) {
      return compfinds('R', seq('role','R'), seq(), rules)
    },

    findinits: function (rules) {
      return compfinds('P', seq('init','P'), seq(), rules)
    },

    findlegals: function (role, facts, rules) {
      return compfinds('X', seq('legal',role,'X'), facts, rules)
    },

    findnexts: function (facts, rules) {
      return compfinds('P', seq('next','P'), facts, rules).sort()
    },

    findterminal: function (facts, rules) {
      return compfindp('terminal', facts, rules)
    },

    findreward: function (role, facts, rules) {
      return compfindx('R', seq('goal',role,'R'), facts, rules)
    }  
  }
}

function load_machine (rules) {
  var my_state_machine = Object.create(StateMachine);
  my_state_machine.initialize(rules);
  return my_state_machine;
}