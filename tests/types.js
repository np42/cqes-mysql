const { Match } = require('../type');

const p1 = Match.from
( { $: 'MatchOr'
  , or: [ { $: 'MatchAnd', and:
            [ { $: 'MatchField', field: 'toto', operator: 'equiv', value: 42 }
            , { $: 'MatchField', field: 'titi', operator: 'in', value: ['a', 'b', 'c'] }
            ]
          }
        , { $: 'MatchField', field: 'tutu', operator: 'equiv', value: 500 }
        ]
  }
);

const { requirements, parameters } = Match.compute(p1);

console.log(requirements);
console.log(parameters);
