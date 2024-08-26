import os
import re
import logging
from datetime import datetime
from typing import Union, Any
import mysql.connector as connector
from mysql.connector import errorcode
from tools.config import Config, DbConfig
from tools.meetings_tools import get_meeting_docs
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.abstracts import  MySQLConnectionAbstract

logger = logging.getLogger(__name__)


class SqlQueryManager:
    def __init__(self,
                 db_config: Config = DbConfig(),
                 charset: str = "utf8mb4",
                 collation: str = "utf8mb4_unicode_ci"):
        self.db_config = db_config
        self.charset = charset
        self.collation = collation

    def __enter__(self):
        logger.debug(f"Creating DB connection to {self.db_config.host}:{self.db_config.port} for user {self.db_config.user}")
        db_conn = connector.connect(host=self.db_config.host, port=self.db_config.port, user=self.db_config.user,
                                    password=self.db_config.password, charset=self.charset, collation=self.collation)
        self.db_conn = db_conn
        self.db_cursor = self.db_conn.cursor()
        return self

    def execute(self, query: str, params = (), set_default_database=True) -> Any:
        if set_default_database:
            self.db_cursor.execute(f"USE {self.db_config.database_name}")
        return self.db_cursor.execute(query, params)

    def executemany(self, query: str, data: list[Any]) -> Any:
        self.db_cursor.execute(f"USE {self.db_config.database_name}")
        return self.db_cursor.executemany(query, data)

    def fetchall(self):
        return self.db_cursor.fetchall()

    def __exit__(self, type, value, traceback):
        if self.db_conn is not None:
            self.db_cursor.close()
            self.db_conn.close()


def get_tables_schema():
    tables = {}
    tables["meetings"] = (
        "CREATE TABLE `meetings` ("
        " `number` int(4) NOT NULL,"
        " `meeting_date` date NOT NULL,"
        " `start_time` time NOT NULL,"
        " `end_time` time NOT NULL,"
        " `time_zone` CHAR(3) NOT NULL,"
        " PRIMARY KEY (`number`)"
        ") ENGINE=InnoDB"
    )
    tables["subjects"] = (
        "CREATE TABLE `meeting_subjects` ("
        " `name` varchar(100) NOT NULL,"
        " `meeting_number` int(4) NOT NULL,"
        " PRIMARY KEY (`name`, `meeting_number`),"
        " CONSTRAINT `subjects_meeting_fk` FOREIGN KEY(`meeting_number`)"
        " REFERENCES `meetings` (`number`) ON DELETE CASCADE"
        ") ENGINE=InnoDB"
    )
    """
    tables["conversations"] = (
        "CREATE TABLE `meeting_conversations` ("
        " `id` int(6) NOT NULL AUTO_INCREMENT,"
        " `vector_id` bigint NULL,"
        " `content` text NOT NULL,"
        " `meeting_number` int(4) NOT NULL,"
        " `speaker` varchar(50) NULL,"
        " PRIMARY KEY (`id`),"
        " CONSTRAINT `conversations_meetings_fk` FOREIGN KEY (`meeting_number`)"
        " REFERENCES `meetings` (`number`) ON DELETE CASCADE"
        ") ENGINE=InnoDB"
    )
    """
    tables["meeting_summaries"] = (
        "CREATE TABLE `meeting_summaries` ("
        " `id` int(6) NOT NULL AUTO_INCREMENT,"
        " `vector_id` bigint NULL,"
        " `summary` text NOT NULL,"
        " `meeting_number` int(4) NOT NULL,"
        " `speaker` varchar(50) NULL,"
        " PRIMARY KEY (`id`),"
        " CONSTRAINT `summaries_meetings_fk` FOREIGN KEY (`meeting_number`)"
        " REFERENCES `meetings` (`number`) ON DELETE CASCADE"
        ") ENGINE=InnoDB"
    )

    return tables


def _create_tables(query_manager: SqlQueryManager) -> None:
    tables = get_tables_schema()

    for table_name, table_desc in tables.items():
        try:
            logger.info(f"Creating DB table {table_name} ...")
            query_manager.execute(table_desc)
        except connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logger.error(f"DB table {table_name} already exists.")
            else:
                logger.error(err.msg)
        else:
            logger.info(f"DB table {table_name} successfully created.")


def parse_db_schema(sql: str) -> dict:
    table_pattern = r"CREATE TABLE \`(?P<table>\w+)\`\s+\((?P<definition>[^;]+);"
    column_pattern = r"\s+`(?P<column>\w+)`\s+(?P<type>\w+)"
    table_regex = re.compile(table_pattern, flags=re.IGNORECASE)
    column_regex = re.compile(column_pattern, flags=re.IGNORECASE)
    tables_found = table_regex.findall(sql)
    tables_definition = {}
    for table in tables_found:
        table_name, table_definition = table
        columns = tables_definition.get(table_name, [])
        columns_found = column_regex.findall(table_definition)
        columns_found = [c for c in columns_found if c[1].lower() != "foreign"]
        _ = [columns.append(c) for c in columns_found]
        tables_definition[table_name] = columns

    return tables_definition


def insert_meetings(meetings: list[dict], query_manager: SqlQueryManager) -> None:
    if len(meetings) == 0:
        logger.info("There are no meetings to insert.")
        return

    new_meetings = _get_new_meetings(meetings, query_manager)

    meetings_data = [(
        m["number"],datetime.strptime(m["date"], "%Y-%m-%d"),datetime.strptime(m['start_time'], "%H:%M"),
        datetime.strptime(m['end_time'], "%H:%M"),m["time_zone"])
        for m in new_meetings]
    sql = ("INSERT INTO meetings (number, date, start_time, end_time, time_zone) "
           "VALUES (%s, %s, %s, %s, %s)")
    query_manager.executemany(sql, meetings_data)


def insert_meeting_subjects(meetings: list[dict], query_manager: SqlQueryManager) -> None:
    if len(meetings) == 0:
        logger.info("There are no meeting subjects to insert.")
        return

    new_subjects = _get_new_subjects(meetings, query_manager)

    query_manager.execute("SET FOREIGN_KEY_CHECKS=0")
    sql = ("INSERT INTO meeting_subjects (name, meeting_number) "
           "VALUES (%s, %s)")
    params = list(new_subjects)
    query_manager.executemany(sql, params)
    query_manager.execute("SET FOREIGN_KEY_CHECKS=1")


def insert_meeting_conversations(meetings: list[dict], query_manager: SqlQueryManager) -> None:
    if len(meetings) == 0:
        logger.info("No meeting conversation inserted, meetings list is empty.")
        return

    sql = (
        "INSERT INTO conversations (content, meeting_number, speaker) "
        " VALUES(%s, %s, %s)"
    )
    conversations_data = sum([[[doc["text"], m["number"], doc["speaker"]]
                               for doc in get_meeting_docs(m)]
                              for m in meetings], [])

    query_manager.executemany(sql, conversations_data)


def insert_meeting_summaries(meetings_summaries: list[tuple[int, str, int, str]],
                                           query_manager: SqlQueryManager) -> None:
    sql = (
        "INSERT INTO meeting_summaries (vector_id, summary, meeting_number, speaker) "
        " VALUES(%s, %s, %s, %s)"
    )
    query_manager.executemany(sql, meetings_summaries)


def _get_new_meetings(meetings: list[dict], query_manager: SqlQueryManager) -> list[dict]:
    meeting_nums = [m["number"] for m in meetings]
    values_placeholders = ",".join(["%s"] * len(meeting_nums))
    select_existing_meeting_nums_sql = ("SELECT number FROM meetings "
                                        f"WHERE number in ({values_placeholders})")
    existing_meeting_nums = []
    query_manager.execute(select_existing_meeting_nums_sql, meeting_nums)
    for row in query_manager.fetchall():
        existing_meeting_nums.append(row[0])
    new_meeting_nums = list(set(meeting_nums) - set(existing_meeting_nums))
    new_meetings = [m for m in meetings if m["number"] in new_meeting_nums]

    return new_meetings


def _get_new_subjects(meetings: list[dict], query_manager: SqlQueryManager) -> set[tuple[str, int]]:
    # Make flat list of subject-meeting pairs.
    subjects = sum([sum([[name, m["number"]] for name in m["subjects"]], []) for m in meetings], [])
    values_placeholders = ",".join(["(%s, %s)"] * int(len(subjects)/2))
    sql = (f"SELECT name, meeting_number FROM meeting_subjects "
           f"WHERE (name, meeting_number) IN ({values_placeholders})")
    query_manager.execute(sql, subjects)
    existing_subjects_set = set()
    for subject_record in query_manager.fetchall():
        existing_subjects_set.add((subject_record[0], subject_record[1]))
    subjects_set = set([(subjects[i], subjects[i + 1]) for i in range(0, len(subjects) - 1, 2)])
    new_subjects = subjects_set - existing_subjects_set

    return new_subjects


def get_conversations(db_name: str, query_manager: SqlQueryManager) -> list[dict]:
    pass


def get_meeting_summaries(vector_ids: list[int], query_manager: SqlQueryManager) -> list[str]:
    if len(vector_ids) == 0:
        logger.info("No summaries retrieved, vector_ids list is empty.")
        return []

    values_placeholders = ",".join(["%s"] * len(vector_ids))
    sql = ("SELECT summary FROM meeting_summaries "
           f"WHERE vector_id IN ({values_placeholders})")
    logger.debug(f"SQL: {sql}")
    query_manager.execute(sql, vector_ids)
    summaries = [r[0] for r in query_manager.fetchall()]

    return summaries


def init_db(query_manager: SqlQueryManager) -> None:
    db_config = DbConfig()
    query_manager.execute("SHOW DATABASES", set_default_database=False)
    for db in query_manager.fetchall():
        if db[0] == db_config.database_name:
            logger.debug(f"Database {db_config.database_name} already exists")
            return
    logger.debug(f"Creating database {db_config.database_name}")
    query_manager.execute(f"CREATE DATABASE {db_config.database_name}", set_default_database=False)
    _create_tables(query_manager)


if __name__ == "__main__":
    with open(os.path.join(os.getcwd(), "..", "..", "data", "data.sql"), "r") as fp:
        db_schema_sql = fp.read()
    table_definitions = parse_db_schema(db_schema_sql)
    for table_name, table_columns in table_definitions.items():
        print(f"table: {table_name}")
        print(f"Columns: {table_columns}")
