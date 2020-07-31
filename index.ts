import { Component }  from 'cqes';
import { merge }      from 'cqes-util';
import * as mysql     from 'mysql';

export function parseURL(url: string) {
  const object = new URL(url);
  const result = <props>{};
  if (object.protocol !== 'mysql:') return result;
  if (object.hostname) result.host = object.hostname;
  if (object.port) result.port = Number(object.port);
  if (object.username) result.user = object.username;
  if (object.password) result.password = object.password;
  if (object.pathname != '/') result.database = object.pathname.substr(1);
  return result;
}

export interface SQLResponse {
  id:       string;
  revision: number;
}

export interface SQLConnection {
  url?:                string;
  host?:               string;
  port?:               number;
  user?:               string;
  password?:           string;
  database?:           string;
  waitForConnections?: boolean;
  lifetime?:           number;
  typeCast?:           any;
  connectionLimit?:    number;
}

export interface props extends Component.props, SQLConnection {
}

interface SQLQueryCallback {
  (error: Error, result?: Array<any>, fields?: any): void;
}

interface SQLTransactCallback {
  (error: Error, result?: any): void;
}

interface SQLTransacQueries {
  (requester: SQLRequester): Promise<any>;
}

interface SQLRequester {
  (query: string, params: Array<any>): Promise<any>;
}

export function escape(value: any) {
  return mysql.escape(value);
}

export function escapeId(name: string) {
  return mysql.escapeId(name);
}

export class MySQL extends Component.Component {
  protected pool: mysql.Pool;
  protected connection: SQLConnection;

  static escape(value: any) {
    return escape(value);
  }

  static escapeId(name: string) {
    return escapeId(name);
  }

  static toSQL(value: any) {
    if (value instanceof Date) {
      const date = value.toISOString().substr(0, 10);
      const h = value.getHours();
      const m = value.getMinutes();
      const s = value.getSeconds();
      const time = [h < 10 ? '0' + h : h, m < 10 ? '0' + m : m, s < 10 ? '0' + s : s].join('-');
      return date + ' ' + time;
    }
    return value;
  }

  constructor(props: props) {
    super(props);
    this.connection = merge(props.url ? parseURL(props.url) : {}, props);
    if (this.connection.waitForConnections !== false)
      this.connection.waitForConnections = true;
    if (!(this.connection.lifetime > 0))
      this.connection.lifetime = 600000;
    this.connection.typeCast = function (field: any, next: any) {
      if (field.type === 'TIME' || field.type === 'DATE') {
        return field.string();
      } else {
        return next();
      }
    };
    this.pool = mysql.createPool(this.connection);
  }

  public start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.pool.getConnection((err, connection) => {
        if (err) {
          return reject(err);
        } else {
          const timedConnection = <any>connection;
          timedConnection.createdAt = Date.now();
          connection.release();
          return resolve();
        }
      });
    });
  }

  public getConnection(handler: (err: Error, connection: mysql.PoolConnection) => void) {
    this.pool.getConnection((err: Error, connection: mysql.PoolConnection) => {
      const timedConnection = <any>connection;
      if (err) {
        if (connection) connection.destroy();
        handler(err, null);
      } else if (timedConnection.createdAt == null) {
        timedConnection.createdAt = Date.now();
        handler(null, connection);
      } else {
        const age = Date.now() - timedConnection.createdAt;
        if (age > this.connection.lifetime) {
          connection.destroy();
          this.getConnection(handler);
        } else {
          handler(null, connection);
        }
      }
    });
  }

  public request(query: string, params: Array<any>): Promise<Array<any>> {
    const self = this; // fix mysql wrap context
    return new Promise((resolve, reject) => {
      return this.getConnection((err, connection) => {
        if (err) return reject(err);
        const request = connection.query(query, params.map(MySQL.toSQL), (err, result, fields) => {
          if (err && err.fatal) connection.destroy();
          else connection.release();
          this.logger.log('%s', request.sql);
          if (err) return reject(err);
          Object.defineProperty(result, 'fields', { value: fields });
          return resolve(result);
        });
      });
    });
  }

  public transact(queries: SQLTransacQueries, cb?: SQLTransactCallback): Promise<void> {
    if (cb == null) cb = (e: Error) => { if (e) this.logger.error('Transaction failed with:', e); };
    return new Promise(resolve => {
      return this.getConnection((err, connection) => {
        if (err) return cb(err);
        return connection.beginTransaction(async err => {
          if (err) return cb(err);
          let result = <any>null;
          try {
            result = await queries((query: string, rawParams: Array<any>) => {
              return new Promise((resolve, reject) => {
                const startAt = Date.now();
                const params  = rawParams.map(MySQL.toSQL);
                const request = connection.query(query, params, (err: Error, result: Array<any>) => {
                  this.logger.log('(%sms)', Date.now() - startAt, request.sql);
                  if (err) return reject(err);
                  else return resolve(result);
                });
              });
            });
          } catch (err) {
            this.logger.error('Transaction query failed with:', err);
            return connection.rollback(() => cb(err));
          }
          return connection.commit(err => {
            if (err && err.fatal) connection.destroy();
            else connection.release();
            resolve();
            if (err) return cb(err);
            else return cb(null, result);
          });
        });
      });
    });
  }


  public stop(): Promise<void> {
    return new Promise(resolve => {
      this.pool.end(() => {
        resolve();
      })
    });
  }

}
