import * as CQES  from 'cqes';
import * as mysql from 'mysql';

export interface props extends CQES.Repository.props {
  connection: {
    host:                string,
    user:                string,
    password:            string,
    database:            string,
    waitForConnections?: boolean,
    lifetime?:           number
  };
  table?: string;
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

export interface children extends CQES.Repository.children {}

export class Repository extends CQES.Repository.Repository {
  protected connection:  mysql.Pool;

  static escape(value: any) {
    return mysql.escape(value);
  }

  static escapeId(name: string) {
    return mysql.escapeId(name);
  }

  constructor(props: props, children: children) {
    super({ ...props, type: props.type + '.mysql' }, children);
    if (props.connection == null)
      props.connection = { host: '127.0.0.1', user: 'root', password: '', database: 'mysql' };
    if (props.connection.waitForConnections !== false)
      props.connection.waitForConnections = true;
    if (!(props.connection.lifetime > 0))
      props.connection.lifetime = 600000;
    this.connection = mysql.createPool(props.connection);
  }

  protected getConnection(handler: (err: Error, connection: mysql.PoolConnection) => void) {
    this.connection.getConnection((err: Error, connection: mysql.PoolConnection) => {
      const timedConnection = <any>connection;
      if (err) {
        if (connection) connection.destroy();
        handler(err, null);
      } else if (timedConnection.createdAt == null) {
        timedConnection.createdAt = Date.now();
        handler(null, connection);
      } else {
        const age = Date.now() - timedConnection.createdAt;
        if (age > this.props.connection.lifetime) {
          connection.destroy();
          this.getConnection(handler);
        } else {
          handler(null, connection);
        }
      }
    });
  }

  protected request(query: string, params: Array<any>): Promise<Array<any>> {
    return new Promise((resolve, reject) => {
      return this.getConnection((err, connection) => {
        if (err) return reject(err);
        const request = connection.query(query, params, (err, result, fields) => {
          if (err && err.fatal) connection.destroy();
          else connection.release();
          this.logger.log(request.sql);
          if (err) return reject(err);
          Object.defineProperty(result, 'fields', { value: fields });
          return resolve(result);
        });
      });
    });
  }

  protected transact(queries: SQLTransacQueries, cb?: SQLTransactCallback): Promise<void> {
    if (cb == null) cb = (e: Error) => { if (e) this.logger.error('Transaction failed with:', e); };
    return new Promise(resolve => {
      return this.getConnection((err, connection) => {
        if (err) return cb(err);
        return connection.beginTransaction(async err => {
          if (err) return cb(err);
          let result = <any>null;
          try {
            result = await queries((query: string, params: Array<any>) => {
              return new Promise((resolve, reject) => {
                const request = connection.query(query, params, (err: Error, result: Array<any>) => {
                  this.logger.log(request.sql);
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

  //--
  public start(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this.connection.getConnection((err, connection) => {
        if (err) {
          this.logger.error(err);
          return resolve(false);
        } else {
          const timedConnection = <any>connection;
          timedConnection.createdAt = Date.now();
          connection.release();
          resolve(true);
        }
      });
    });
  }

  public stop(): Promise<void> {
    return new Promise(resolve => this.connection.end(() => resolve()));
  }

}
