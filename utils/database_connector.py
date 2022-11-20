import logging

class DatabaseConnector():
    def __init__(self):
        import json
        conn_config = json.load(open('./config/db_internal.json'))
        self.engine = self._create_sqlalchemy_engine(conn_config['username'], conn_config['password'], conn_config['pghost'],
                                           conn_config['pgport'], conn_config['pgdatabase'], conn_config['pgschema'])

    @staticmethod
    def _create_sqlalchemy_engine(user, password, host, port, database, schema):
        from sqlalchemy import create_engine
        return create_engine(
            f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}',
            connect_args={'options': '-csearch_path=' + schema})

    def csv_to_db_raw(self, df_original, postgre_table):
        postgre_table = f"raw_{postgre_table}"
        self._df_to_psql(df_original, postgre_table)

    def query_no_result(self, query_string):
        try:
            self.engine.execute(query_string)
        except (Exception) as error:
            print("Probably syntax error", error)
            raise

    def query_to_df(self, query_string):
        import pandas as pd
        data, colnames = self._query(query_string)
        df = pd.DataFrame(data)
        if df.empty:
            df = pd.DataFrame(columns=colnames)
        else:
            df.columns = colnames
        return df

    def clean_up(self):
        '''DO
        $do$
            DECLARE
                _tbl text;
            BEGIN
                FOR _tbl  IN
                    SELECT quote_ident(table_schema) || '.'
                               || quote_ident(table_name)      -- escape identifier and schema-qualify!
                    FROM   information_schema.tables
                    WHERE  table_name LIKE 'raw_' || '%'  -- your table name prefix
                      AND    table_schema NOT LIKE 'pg\_%'    -- exclude system schemas
                    LOOP
                        EXECUTE 'DROP TABLE ' || _tbl;  -- see below
                    END LOOP;
            END
        $do$;'''
        #fancy function doesnt work with create engine, so good old fashioned hand job must do #TODO fix this
        sql = '''
        drop table if exists raw_data;
        drop table if exists "raw_NationalNames";
        drop table if exists "raw_StateNames";
        '''
        self.query_no_result(sql)


    def _query(self, query_string):
        from sqlalchemy.exc import ResourceClosedError
        try:
            result_set = self.engine.execute(query_string).fetchall()
            #print([col for col in self.engine.execute(query_string).keys()])
            colnames = [col for col in self.engine.execute(query_string).keys()]
        except(ResourceClosedError):
            return None  # This result object does not return rows. eg update
        except (Exception) as error:
            print("Probably syntax error", error)
            raise
        return result_set, colnames

    def _df_to_psql(self, df_original, postgre_table):
        from io import StringIO
        import psycopg2
        df_original.fillna('\\N', inplace=True)  # so NULL is real NULL in Postgre
        try:
            df_original.head(0).to_sql(postgre_table, self.engine, if_exists='replace', index=False)  # truncates the table
            logging.info(f'Temp table {postgre_table} created')
        except (Exception, psycopg2.DatabaseError) as e:
            logging.error(e)
            logging.error(f'Failed to create temp table {postgre_table}')
            return 1

        conn = self.engine.raw_connection()
        cur = conn.cursor()
        output = StringIO()
        df_original.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        try:
            cur.copy_from(output, postgre_table)  # must be escaped
            conn.commit()
            logging.info(f'Temp table {postgre_table} populated with data')
        except (Exception, psycopg2.DatabaseError) as e:
            conn.rollback()
            logging.error(f'Failed to populate temp table {postgre_table}')
            logging.error(e)
            return 1
        finally:
            conn.close()






