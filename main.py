import os
import json
import time
from loki_handler import logger
from kafka import KafkaConsumer
from configparser import ConfigParser
from oracle_connector import OracleDB
import oracledb

URL_CONF = 'config/config.ini'

try:
    APEX_USER = os.environ["APEX_USER"]
except KeyError:
    config = ConfigParser()
    config.read(URL_CONF)
    APEX_USER = config['db']['USER']

try:
    APEX_PASSWORD = os.environ["APEX_PASSWORD"]
except KeyError:
    config = ConfigParser()
    config.read(URL_CONF)
    APEX_PASSWORD = config['db']['PASSWORD']

try:
    APEX_DSN = os.environ["APEX_DSN"]
except KeyError:
    config = ConfigParser()
    config.read(URL_CONF)
    APEX_DSN = config['db']['DSN']

try:
    KAFKA_BOOTSTRAP_SERVER = os.environ["KAFKA_BOOTSTRAP_SERVER"]
except KeyError:
    KAFKA_BOOTSTRAP_SERVER = 'oraapex-prod.gksm.local:9092'

TOPIC = ['OS_USERS_CONNECTED_AS_DBA']
GROUP_ID = 'OS_USERS_CONNECTED_AS_DBA_CONSUMER'

db = OracleDB(
    user=APEX_USER,
    password=APEX_PASSWORD,
    dsn=APEX_DSN,
    max_connections=6,
    min_connections=3,
    increment=1
)


def get_sql_scripts(connection):
    sqls = {}
    with connection.cursor() as cursor:
        for r in cursor.execute("SELECT s.CODE, s.SCRIPT "
                                "FROM APP_APEX_MICROSERVICES.V_PYTHON_SQL s "
                                "WHERE s.PROJECT_NAME = 'os_users_connected_as_dba' "
                                "ORDER BY s.ID "):
            sqls[str(r[0])] = ''.join(r[1].read())
    return sqls


sql_list = get_sql_scripts(db.get_connection())

consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                         group_id=GROUP_ID,
                         auto_offset_reset="earliest",
                         enable_auto_commit=False)

consumer.subscribe(TOPIC)

for message in consumer:
    try:
        consumer.commit()
    except Exception as e:
        log_msg = {
            "msg": "Kafka commit error, continue " + str(e)
        }
        logger.debug(log_msg)
        time.sleep(1)
        continue

    try:
        JSON = json.loads(message.value.decode('utf-8'))
        dbid = JSON.get('DBID')
        rows_db = []
        error_text = None
        if dbid is not None:
            rows = db.execute_query_and_fetchall(sql_list['OS_USERS_CONNECTED_AS_DBA_SELECT_DB_PASS_DSN'],
                                                 params={"dbid": dbid})
            if not rows:
                continue
            for row in rows:
                db.execute_query_and_commit(sql_list['OS_USERS_CONNECTED_AS_DBA_START_DB'],
                                            params={"dbid": dbid})
                log_msg = {
                    "msg": "started DB:" + str(dbid)
                }
                logger.info(log_msg)
                if len(rows) == 1:
                    try:
                        db_sys = OracleDB(
                            user='SYS',
                            password=row[0],
                            dsn=row[1],
                            max_connections=2,
                            min_connections=2,
                            increment=1,
                            mode=oracledb.SYSDBA
                        )
                        rows_db = db_sys.execute_query_and_fetchall(
                            sql_list['OS_USERS_CONNECTED_AS_DBA_TABLES_SELECT_RESULTS'])
                        status = 'success'
                    except Exception as e:
                        rows_db.clear()
                        status = 'error'
                        try:
                            error_text = str(e)
                        except Exception as ex:
                            error_text = 'Unknown Error'
                            log_msg = {
                                "msg": error_text
                            }
                            logger.error(log_msg)

                    for row_db in rows_db:
                        try:
                            db.execute_query_and_commit(sql_list['OS_USERS_CONNECTED_AS_DBA_INSERT_RESULTS'],
                                                        params={"dbid": dbid,
                                                                "unified_audit_policies": row_db[0],
                                                                "os_username": row_db[1],
                                                                "userhost": row_db[2],
                                                                "client_program_name": row_db[3],
                                                                "current_user": row_db[4],
                                                                "records": row_db[5],
                                                                "connect_date": row_db[6]
                                                                })
                        except Exception as e:
                            rows_db.clear()
                            status = 'error'
                            try:
                                error_text = str(e)
                            except Exception as ex:
                                error_text = 'Unknown Error'
                                log_msg = {
                                    "msg": error_text
                                }
                                logger.error(log_msg)

                    db.execute_query_and_commit(sql_list['OS_USERS_CONNECTED_AS_DBA_INSERT_RES_CONN'],
                                                params={"dbid": dbid,
                                                        "status": str(status),
                                                        "error_text": error_text
                                                        })
                    log_msg = {
                        "msg": 'finished DB: ' + str(dbid)
                    }
                    logger.info(log_msg)
    except Exception as e:
        log_msg = {
            "msg": f"Error occurred while consuming messages: {str(e)}"}
        logger.error(log_msg)
    finally:
        db.close()
