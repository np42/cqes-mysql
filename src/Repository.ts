import * as CQES  from 'cqes';
import * as mysql from 'mysql';

export interface Props extends CQES.Repository.Props {
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

export interface Children extends CQES.Repository.Children {}

export class Repository extends CQES.Repository.Repository {
  protected connection:  mysql.Pool;
  protected running:     number;

  constructor(props: Props, children: Children) {
    super({ type: 'Repository.MySQL', color: 'blue', ...props }, children);
    if (props.connection == null)
      props.connection = { host: '127.0.0.1', user: 'root', password: '', database: 'mysql' };
    if (props.connection.waitForConnections !== false)
      props.connection.waitForConnections = true;
    if (!(props.connection.lifetime > 0))
      props.connection.lifetime = 600000;
    this.connection = mysql.createPool(props.connection);
  }

  protected request(query: string, params: Array<any>, cb?: SQLQueryCallback) {
    if (cb == null) cb = e => null;
    return this.connection.getConnection((err, connection) => {
      if (err) this.request(query, params, cb);
      const timedConnection = <any>connection;
      if (timedConnection.createdAt == null) timedConnection.createdAt = Date.now();
      const request = connection.query(query, params, (err, result, fields) => {
        if (timedConnection.createdAt + this.props.connection.lifetime > Date.now())
          connection.destroy();
        else
          connection.release();
        this.logger.log(request.sql);
        if (err && err.fatal) {
          this.logger.error(err);
          return this.request(query, params, cb);
        } else {
          if (cb != null) return cb(err, result, fields);
        }
      });
    });
  }

  protected transact(queries: SQLTransacQueries, cb?: SQLTransactCallback): Promise<void> {
    if (cb == null) cb = e => null;
    return new Promise(resolve => {
      this.connection.getConnection((err, connection) => {
        if (err) { this.logger.error(err); return cb(err); }
        const timedConnection = <any>connection;
        if (timedConnection.createdAt == null) timedConnection.createdAt = Date.now();
        return connection.beginTransaction(async err => {
          if (err) { this.logger.error(err); return cb(err); }
          const requester = (query: string, params: Array<any>) => {
            return new Promise((resolve, reject) => {
              const request = connection.query(query, params, (err: Error, result: Array<any>) => {
                this.logger.log(request.sql);
                if (err) return reject(err);
                else return resolve(result);
              });
            });
          };
          let result = <any>null;
          try { result = await queries(requester); }
          catch (err) {
            this.logger.error(err);
            return connection.rollback(() => cb(err));
          }
          return connection.commit(err => {
            if (timedConnection.createdAt + this.props.connection.lifetime > Date.now())
              connection.destroy();
            else
              connection.release();
            resolve();
            if (err) { this.logger.error(err); return cb(err); }
            return cb(null, result);
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
