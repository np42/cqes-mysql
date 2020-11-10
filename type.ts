import { escapeId } from './index';
import { Array, Sum, Object, Enum, String, Value } from 'cqes-type';

export class Match extends Sum
  .either('MatchField', () => MatchField)
  .either('MatchAnd',   () => MatchAnd)
  .either('MatchOr',    () => MatchOr)
{

  static compute(value: Match) {
    let requirements = '';
    const parameters: Array<any> = [];
    switch (value.$) {
    case 'MatchField': {
      const result = MatchField.compute(<MatchField> value);
      requirements += result.requirements;
      parameters.splice(Infinity, 0, ...result.parameters);
    } break ;
    case 'MatchOr': {
      const result = MatchOr.compute(<MatchOr> value);
      requirements += result.requirements;
      parameters.splice(Infinity, 0, ...result.parameters);
    } break ;
    case 'MatchAnd': {
      const result = MatchAnd.compute(<MatchAnd> value);
      requirements += result.requirements;
      parameters.splice(Infinity, 0, ...result.parameters);
    } break ;
    }
    return { requirements, parameters };
  }

}

export class MatchField extends Object
  .add('field',    String)
  .add('operator', Enum.as('equiv').as('in'))
  .add('value',    Value)
{
  field:    string;
  operator: 'equiv' | 'in';
  value:    any;

  static compute(value: MatchField) {
    switch (value.operator) {
    case 'equiv':
      return { requirements: escapeId(value.field) + ' = ?'
             , parameters: [value.value]
             };
    case 'in':
      return { requirements: escapeId(value.field) + ' IN (?)'
             , parameters: [value.value]
             };
    }
  }

}

export class MatchAnd extends Object
  .add('and', Array(Match))
{
  and: Array<$Match>;

  static compute(value: MatchAnd) {
    let requirements = '( ';
    const parameters: Array<any> = [];
    for (let i = 0; i < value.and.length; i += 1) {
      const result = Match.compute(value.and[i]);
      if (i > 0) requirements += ' AND ';
      requirements += result.requirements;
      parameters.splice(Infinity, 0, ...result.parameters);
    }
    requirements += ' )';
    return { requirements, parameters };
  }

}

export class MatchOr extends Object
  .add('or', Array(Match))
{
  or: Array<$Match>;

  static compute(value: MatchOr) {
    let requirements = '( ';
    const parameters: Array<any> = [];
    for (let i = 0; i < value.or.length; i += 1) {
      const result = Match.compute(value.or[i]);
      if (i > 0) requirements += ' OR ';
      requirements += result.requirements;
      parameters.splice(Infinity, 0, ...result.parameters);
    }
    requirements += ' )';
    return { requirements, parameters };
  }

}

export type $Match = MatchField | MatchAnd | MatchOr;
