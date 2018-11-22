import * as CQES from 'cqes';
import * as mysql from 'mysql';

export interface Config {
  name: string;
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

export class Repository {

  protected logger:      CQES.Logger;
  protected connection:  mysql.Pool;
  protected running:     number;
  protected config:      Config;

  protected init(config: Config) {
    this.logger     = new CQES.Logger(config.name + '.Repository.MySQL', 'blue');
    if (config.connection.waitForConnections !== false)
      config.connection.waitForConnections = true;
    if (!(config.connection.lifetime > 0))
      config.connection.lifetime = 600000;
    this.connection = mysql.createPool(config.connection);
    this.config     = config;
  }

  protected start() {
    return new Promise((resolve, reject) => {
      this.connection.getConnection((err, connection) => {
        if (err) return reject(false);
        connection.release();
        resolve(true);
      });
    });
  }

  protected stop() {
    return new Promise(resolve => this.connection.end(resolve));
  }

  protected query(query: string, params: Array<any>, cb?: (error: Error, result: Array<any>) => void) {
    return this.connection.getConnection((err, connection) => {
      if (err) this.query(query, params, cb);
      const timedConnection = <any>connection;
      if (timedConnection.createdAt == null) timedConnection.createdAt = Date.now();
      connection.query(query, params, (err, result) => {
        if (timedConnection.createdAt + this.config.connection.lifetime > Date.now())
          connection.destroy();
        else
          connection.release();
        if (err && err.fatal) {
          this.logger.error(err);
          return this.query(query, params, cb);
        } else {
          if (cb != null) return cb(err, result);
        }
      });
    });
  }

  protected load(key: string) {
    const table = mysql.escapeId(this.config.table);
    return new Promise((resolve, reject) => {
      this.query
      ( [ 'SELECT `version`, `status`, `data` FROM ' + table
        , 'WHERE `key` = ?'
        ].join(' ')
      , [ key ]
      , (error, [entry]) => {
        if (error) return reject(error);
        if (entry == null) return resolve(new CQES.State());
        return resolve(new CQES.State(entry.version, entry.status, JSON.parse(entry.data)));
      });
    });
  }

  protected save(key: string, state: CQES.State) {
    const table = mysql.escapeId(this.config.table);
    return this.query
    ( [ 'INSERT INTO ' + table + ' (`key`, `version`, `status`, `data`) VALUES (?, ?, ?, ?)'
      , 'ON DUPLICATE KEY UPDATE `version` = ?, `status` = ?, `data` = ?'
      ].join(' ')
    , [ key, state.version, state.status, JSON.stringify(state.data)
      , state.version, state.status, JSON.stringify(state.data)
      ]
    );
  }

  protected resolve(query: CQES.Query): Promise<CQES.Reply> {
    const reply = new CQES.Reply(CQES.ReplyStatus.Rejected, new Error("Not implemented"));
    return Promise.resolve(reply);
  }

}
