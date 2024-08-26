import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock
from tools.db_tools import (
    SqlQueryManager,
    get_tables_schema,
    _create_tables,
    insert_meetings,
    insert_meeting_subjects,
    insert_meeting_conversations,
    insert_meeting_summaries,
    _get_new_meetings,
    _get_new_subjects,
    get_meeting_summaries,
    init_db
)
from tools.config import DbConfig


class TestDbTools(unittest.TestCase):

    @patch("tools.db_tools.connector.connect")
    def test_enter(self, mock_connect):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        db_config = DbConfig()
        with SqlQueryManager(db_config) as manager:
            self.assertEqual(manager.db_conn, mock_conn)
            self.assertEqual(manager.db_cursor, mock_cursor)
            mock_connect.assert_called_once_with(
                host=db_config.host,
                port=db_config.port,
                user=db_config.user,
                password=db_config.password,
                charset="utf8mb4",
                collation="utf8mb4_unicode_ci"
            )

    @patch("tools.db_tools.connector.connect")
    def test_execute(self, mock_connect):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        db_config = DbConfig()
        with SqlQueryManager(db_config) as manager:
            query = "SELECT * FROM test_table"
            params = ()
            manager.execute(query, params)
            mock_cursor.execute.assert_any_call(f"USE {db_config.database_name}")
            mock_cursor.execute.assert_any_call(query, params)

    @patch("tools.db_tools.connector.connect")
    def test_executemany(self, mock_connect):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        db_config = DbConfig()
        with SqlQueryManager(db_config) as manager:
            query = "INSERT INTO test_table (col1, col2) VALUES (%s, %s)"
            data = [(1, "a"), (2, "b")]
            manager.executemany(query, data)
            mock_cursor.execute.assert_called_once_with(f"USE {db_config.database_name}")
            mock_cursor.executemany.assert_called_once_with(query, data)

    @patch("tools.db_tools.connector.connect")
    def test_fetchall(self, mock_connect):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        db_config = DbConfig()
        with SqlQueryManager(db_config) as manager:
            manager.fetchall()
            mock_cursor.fetchall.assert_called_once()

    @patch("tools.db_tools.connector.connect")
    def test_exit(self, mock_connect):
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        db_config = DbConfig()
        manager = SqlQueryManager(db_config)
        manager.__enter__()
        manager.__exit__(None, None, None)
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_get_tables_schema(self):
        expected_schema = {
            "meetings": (
                "CREATE TABLE `meetings` ("
                " `number` int(4) NOT NULL,"
                " `meeting_date` date NOT NULL,"
                " `start_time` time NOT NULL,"
                " `end_time` time NOT NULL,"
                " `time_zone` CHAR(3) NOT NULL,"
                " PRIMARY KEY (`number`)"
                ") ENGINE=InnoDB"
            ),
            "subjects": (
                "CREATE TABLE `meeting_subjects` ("
                " `name` varchar(100) NOT NULL,"
                " `meeting_number` int(4) NOT NULL,"
                " PRIMARY KEY (`name`, `meeting_number`),"
                " CONSTRAINT `subjects_meeting_fk` FOREIGN KEY(`meeting_number`)"
                " REFERENCES `meetings` (`number`) ON DELETE CASCADE"
                ") ENGINE=InnoDB"
            ),
            "meeting_summaries": (
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
        }

        actual_schema = get_tables_schema()
        self.assertEqual(expected_schema, actual_schema)

    @patch("tools.db_tools.get_tables_schema")
    @patch("tools.db_tools.SqlQueryManager")
    def test_create_tables(self, MockSqlQueryManager, mock_get_tables_schema):
        # Mock the table schema
        mock_get_tables_schema.return_value = {
            "test_table": "CREATE TABLE `test_table` (id INT PRIMARY KEY)"
        }

        # Mock the SqlQueryManager instance
        mock_query_manager = MockSqlQueryManager.return_value
        mock_query_manager.execute = MagicMock()

        # Call the function
        _create_tables(mock_query_manager)

        # Assertions
        mock_get_tables_schema.assert_called_once()
        mock_query_manager.execute.assert_called_once_with("CREATE TABLE `test_table` (id INT PRIMARY KEY)")

    @patch("tools.db_tools._get_new_meetings")
    @patch("tools.db_tools.SqlQueryManager")
    def test_insert_meetings(self, MockSqlQueryManager, mock_get_new_meetings):
        # Mock the new meetings data
        mock_get_new_meetings.return_value = [
            {
                "number": 1,
                "date": "2023-10-01",
                "start_time": "09:00",
                "end_time": "10:00",
                "time_zone": "EST"
            }
        ]

        # Mock the SqlQueryManager instance
        mock_query_manager = MockSqlQueryManager.return_value
        mock_query_manager.executemany = MagicMock()

        # Define the meetings data
        meetings = [
            {
                "number": 1,
                "date": "2023-10-01",
                "start_time": "09:00",
                "end_time": "10:00",
                "time_zone": "EST"
            }
        ]

        # Call the function
        insert_meetings(meetings, mock_query_manager)

        # Expected data
        expected_data = [
            (1, datetime.strptime("2023-10-01", "%Y-%m-%d"), 
             datetime.strptime("09:00", "%H:%M"),
             datetime.strptime("10:00", "%H:%M"), "EST")
        ]
        expected_sql = ("INSERT INTO meetings (number, date, start_time, end_time, time_zone) "
                        "VALUES (%s, %s, %s, %s, %s)")

        # Assertions
        mock_get_new_meetings.assert_called_once_with(meetings, mock_query_manager)
        mock_query_manager.executemany.assert_called_once_with(expected_sql, expected_data)

    @patch("tools.db_tools.SqlQueryManager")
    def test_insert_meetings_no_meetings(self, MockSqlQueryManager):
        # Mock the SqlQueryManager instance
        mock_query_manager = MockSqlQueryManager.return_value
        mock_query_manager.executemany = MagicMock()

        # Define the meetings data as empty
        meetings = []

        # Call the function
        insert_meetings(meetings, mock_query_manager)

        # Assertions
        mock_query_manager.executemany.assert_not_called()

    @patch('tools.db_tools._get_new_subjects')
    @patch('tools.db_tools.SqlQueryManager')
    def test_insert_meeting_subjects(self, MockSqlQueryManager, mock_get_new_subjects):
        # Mock the new subjects data
        mock_get_new_subjects.return_value = [
            ("Subject1", 1),
            ("Subject2", 2)
        ]

        # Mock the SqlQueryManager instance
        mock_query_manager = MockSqlQueryManager.return_value
        mock_query_manager.execute = MagicMock()
        mock_query_manager.executemany = MagicMock()

        # Define the meetings data
        meetings = [
            {"number": 1, "subject": "Subject1"},
            {"number": 2, "subject": "Subject2"}
        ]

        # Call the function
        insert_meeting_subjects(meetings, mock_query_manager)

        # Expected data
        expected_data = [
            ("Subject1", 1),
            ("Subject2", 2)
        ]
        expected_sql = ("INSERT INTO meeting_subjects (name, meeting_number) "
                        "VALUES (%s, %s)")

        # Assertions
        mock_get_new_subjects.assert_called_once_with(meetings, mock_query_manager)
        mock_query_manager.execute.assert_any_call("SET FOREIGN_KEY_CHECKS=0")
        mock_query_manager.executemany.assert_called_once_with(expected_sql, expected_data)
        mock_query_manager.execute.assert_any_call("SET FOREIGN_KEY_CHECKS=1")

    @patch('tools.db_tools.SqlQueryManager')
    def test_insert_meeting_subjects_no_meetings(self, MockSqlQueryManager):
        # Mock the SqlQueryManager instance
        mock_query_manager = MockSqlQueryManager.return_value
        mock_query_manager.execute = MagicMock()
        mock_query_manager.executemany = MagicMock()

        # Define the meetings data as empty
        meetings = []

        # Call the function
        insert_meeting_subjects(meetings, mock_query_manager)

        # Assertions
        mock_query_manager.execute.assert_not_called()
        mock_query_manager.executemany.assert_not_called()

    @patch('tools.db_tools.get_meeting_docs')
    @patch('tools.db_tools.SqlQueryManager')
    def test_insert_meeting_conversations(self, MockSqlQueryManager, mock_get_meeting_docs):

        # Mock the meeting documents data
        mock_get_meeting_docs.side_effect = [
            [{"text": "Conversation1", "speaker": "Speaker1"}],
            [{"text": "Conversation2", "speaker": "Speaker2"}]
        ]

        # Mock the SqlQueryManager instance
        mock_query_manager = MockSqlQueryManager.return_value

        mock_query_manager.executemany = MagicMock()

        # Define the meetings data
        meetings = [
            {"number": 1},
            {"number": 2}
        ]

        # Call the function
        insert_meeting_conversations(meetings, mock_query_manager)

        # Expected data
        expected_data = [
            ["Conversation1", 1, "Speaker1"],
            ["Conversation2", 2, "Speaker2"]
        ]
        expected_sql = ("INSERT INTO conversations (content, meeting_number, speaker) "
                        " VALUES(%s, %s, %s)")

        # Assertions
        mock_get_meeting_docs.assert_any_call(meetings[0])
        mock_get_meeting_docs.assert_any_call(meetings[1])
        mock_query_manager.executemany.assert_called_once_with(expected_sql, expected_data)

    @patch('tools.db_tools.SqlQueryManager')
    def test_insert_meeting_conversations_no_meetings(self, MockSqlQueryManager):
        # Mock the SqlQueryManager instance
        mock_query_manager = MockSqlQueryManager.return_value
        mock_query_manager.executemany = MagicMock()

        # Define the meetings data as empty
        meetings = []

        # Call the function
        insert_meeting_conversations(meetings, mock_query_manager)

        # Assertions
        mock_query_manager.executemany.assert_not_called()

    def test_insert_meeting_summaries(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.executemany = MagicMock()

        # Define the meetings summaries data
        meetings_summaries = [
            (1, "Summary1", 1, "Speaker1"),
            (2, "Summary2", 2, "Speaker2")
        ]

        # Call the function
        insert_meeting_summaries(meetings_summaries, mock_query_manager)

        # Expected SQL
        expected_sql = (
            "INSERT INTO meeting_summaries (vector_id, summary, meeting_number, speaker) "
            " VALUES(%s, %s, %s, %s)"
        )

        # Assertions
        mock_query_manager.executemany.assert_called_once_with(expected_sql, meetings_summaries)

    def test_insert_meeting_summaries_empty_list(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.executemany = MagicMock()

        # Define the meetings summaries data as empty
        meetings_summaries = []

        # Call the function
        insert_meeting_summaries(meetings_summaries, mock_query_manager)

        # Expected SQL
        expected_sql = (
            "INSERT INTO meeting_summaries (vector_id, summary, meeting_number, speaker) "
            " VALUES(%s, %s, %s, %s)"
        )

        # Assertions
        mock_query_manager.executemany.assert_called_once_with(expected_sql, meetings_summaries)

    def test_get_new_meetings(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[(1,), (2,)])

        # Define the meetings data
        meetings = [
            {"number": 1, "name": "Meeting1"},
            {"number": 2, "name": "Meeting2"},
            {"number": 3, "name": "Meeting3"}
        ]

        # Call the function
        new_meetings = _get_new_meetings(meetings, mock_query_manager)

        # Expected new meetings
        expected_new_meetings = [
            {"number": 3, "name": "Meeting3"}
        ]

        # Assertions
        self.assertEqual(new_meetings, expected_new_meetings)
        mock_query_manager.execute.assert_called_once_with(
            "SELECT number FROM meetings WHERE number in (%s,%s,%s)",
            [1, 2, 3]
        )
        mock_query_manager.fetchall.assert_called_once()

    def test_get_new_meetings_no_existing(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[])

        # Define the meetings data
        meetings = [
            {"number": 1, "name": "Meeting1"},
            {"number": 2, "name": "Meeting2"},
            {"number": 3, "name": "Meeting3"}
        ]

        # Call the function
        new_meetings = _get_new_meetings(meetings, mock_query_manager)

        # Expected new meetings
        expected_new_meetings = meetings

        # Assertions
        self.assertEqual(new_meetings, expected_new_meetings)
        mock_query_manager.execute.assert_called_once_with(
            "SELECT number FROM meetings WHERE number in (%s,%s,%s)",
            [1, 2, 3]
        )
        mock_query_manager.fetchall.assert_called_once()

    def test_get_new_meetings_all_existing(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[(1,), (2,), (3,)])

        # Define the meetings data
        meetings = [
            {"number": 1, "name": "Meeting1"},
            {"number": 2, "name": "Meeting2"},
            {"number": 3, "name": "Meeting3"}
        ]

        # Call the function
        new_meetings = _get_new_meetings(meetings, mock_query_manager)

        # Expected new meetings
        expected_new_meetings = []

        # Assertions
        self.assertEqual(new_meetings, expected_new_meetings)
        mock_query_manager.execute.assert_called_once_with(
            "SELECT number FROM meetings WHERE number in (%s,%s,%s)",
            [1, 2, 3]
        )
        mock_query_manager.fetchall.assert_called_once()

    def test_get_new_subjects(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[("Subject1", 1), ("Subject2", 2)])

        # Define the meetings data
        meetings = [
            {"number": 1, "subjects": ["Subject1", "Subject3"]},
            {"number": 2, "subjects": ["Subject2", "Subject4"]},
            {"number": 3, "subjects": ["Subject5"]}
        ]

        # Call the function
        new_subjects = _get_new_subjects(meetings, mock_query_manager)

        # Expected new subjects
        expected_new_subjects = {("Subject3", 1), ("Subject4", 2), ("Subject5", 3)}

        # Assertions
        self.assertEqual(new_subjects, expected_new_subjects)
        mock_query_manager.execute.assert_called_once_with(
            "SELECT name, meeting_number FROM meeting_subjects WHERE (name, meeting_number) IN ((%s, %s),(%s, %s),(%s, %s),(%s, %s),(%s, %s))",
            ["Subject1", 1, "Subject3", 1, "Subject2", 2, "Subject4", 2, "Subject5", 3]
        )
        mock_query_manager.fetchall.assert_called_once()

    def test_get_new_subjects_no_existing(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[])

        # Define the meetings data
        meetings = [
            {"number": 1, "subjects": ["Subject1", "Subject3"]},
            {"number": 2, "subjects": ["Subject2", "Subject4"]},
            {"number": 3, "subjects": ["Subject5"]}
        ]

        # Call the function
        new_subjects = _get_new_subjects(meetings, mock_query_manager)

        # Expected new subjects
        expected_new_subjects = {("Subject1", 1), ("Subject3", 1), ("Subject2", 2), ("Subject4", 2), ("Subject5", 3)}

        # Assertions
        self.assertEqual(new_subjects, expected_new_subjects)
        mock_query_manager.execute.assert_called_once_with(
            ("SELECT name, meeting_number FROM meeting_subjects WHERE (name, meeting_number) IN "
             "((%s, %s),(%s, %s),(%s, %s),(%s, %s),(%s, %s))"),
            ["Subject1", 1, "Subject3", 1, "Subject2", 2, "Subject4", 2, "Subject5", 3]
        )
        mock_query_manager.fetchall.assert_called_once()

    def test_get_new_subjects_all_existing(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[
            ("Subject1", 1), ("Subject3", 1), ("Subject2", 2), ("Subject4", 2), ("Subject5", 3)
        ])

        # Define the meetings data
        meetings = [
            {"number": 1, "subjects": ["Subject1", "Subject3"]},
            {"number": 2, "subjects": ["Subject2", "Subject4"]},
            {"number": 3, "subjects": ["Subject5"]}
        ]

        # Call the function
        new_subjects = _get_new_subjects(meetings, mock_query_manager)

        # Expected new subjects
        expected_new_subjects = set()

        # Assertions
        self.assertEqual(new_subjects, expected_new_subjects)
        mock_query_manager.execute.assert_called_once_with(
            ("SELECT name, meeting_number FROM meeting_subjects WHERE (name, meeting_number) IN "
             "((%s, %s),(%s, %s),(%s, %s),(%s, %s),(%s, %s))"),
            ["Subject1", 1, "Subject3", 1, "Subject2", 2, "Subject4", 2, "Subject5", 3]
        )
        mock_query_manager.fetchall.assert_called_once()

    def test_get_meeting_summaries(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[(1,),(2,)])
        vector_ids = [1, 2, 3]

        # Call the function
        summaries = get_meeting_summaries(vector_ids, mock_query_manager)

        # Expected summaries
        expected_summaries = [1, 2]

        # Assertions
        self.assertEqual(summaries, expected_summaries)
        mock_query_manager.execute.assert_called_once_with(
            "SELECT summary FROM meeting_summaries WHERE vector_id IN (%s,%s,%s)",
            vector_ids
        )
        mock_query_manager.fetchall.assert_called_once()

    def test_get_meeting_summaries_empty_vector_ids(self):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[])

        vector_ids = []

        # Call the function
        summaries = get_meeting_summaries(vector_ids, mock_query_manager)

        # Expected summaries
        expected_summaries = []

        # Assertions
        self.assertEqual(summaries, expected_summaries)
        mock_query_manager.execute.assert_not_called()

    @patch('tools.db_tools._create_tables')
    def test_init_db_database_exists(self, mock_create_tables):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[(DbConfig().database_name,)])

        # Call the function
        init_db(mock_query_manager)

        # Assertions
        mock_query_manager.execute.assert_called_once_with("SHOW DATABASES", set_default_database=False)
        mock_query_manager.fetchall.assert_called_once()
        mock_create_tables.assert_not_called()

    @patch('tools.db_tools._create_tables')
    def test_init_db_database_not_exists(self, mock_create_tables):
        # Mock the SqlQueryManager instance
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        mock_query_manager.execute = MagicMock()
        mock_query_manager.fetchall = MagicMock(return_value=[("other_db",)])

        # Call the function
        init_db(mock_query_manager)

        # Assertions
        mock_query_manager.execute.assert_any_call("SHOW DATABASES", set_default_database=False)
        mock_query_manager.execute.assert_any_call(f"CREATE DATABASE {DbConfig().database_name}",
                                                   set_default_database=False)
        mock_query_manager.fetchall.assert_called_once()
        mock_create_tables.assert_called_once_with(mock_query_manager)


if __name__ == "__main__":
    unittest.main()