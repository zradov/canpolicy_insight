import os
import sys
import unittest
from pymilvus import MilvusException
from unittest.mock import patch, mock_open, MagicMock
from tools.persistence_store_builder import (init_meetings_persistence_store,
                                             build_meetings_persistence_store,
                                             load_saved_data,
                                             SqlQueryManager)


class TestPersistenceStoreBuilder(unittest.TestCase):

    @patch("tools.persistence_store_builder.vector_db_tool")
    @patch("tools.persistence_store_builder.init_db")
    def test_init_meetings_persistence_store(self, mock_init_db, mock_vector_db_tool):
        # Arrange
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        auto_id_pk = True

        # Act
        init_meetings_persistence_store(mock_query_manager, auto_id_pk)

        # Assert
        mock_vector_db_tool.init_vectors_store.assert_called_once_with(auto_id_pk)
        mock_init_db.assert_called_once_with(mock_query_manager)

    @patch("tools.persistence_store_builder.vector_db_tool")
    @patch("tools.persistence_store_builder.init_db")
    def test_init_meetings_persistence_store_with_auto_increment_keys_disabled(self, mock_init_db, mock_vector_db_tool):
        # Arrange
        mock_query_manager = MagicMock(spec=SqlQueryManager)
        auto_id_pk = False

        # Act
        init_meetings_persistence_store(mock_query_manager, auto_id_pk)

        # Assert
        mock_vector_db_tool.init_vectors_store.assert_called_once_with(auto_id_pk)
        mock_init_db.assert_called_once_with(mock_query_manager)

    @patch("tools.persistence_store_builder.vector_db_tool")
    @patch("tools.persistence_store_builder.SqlQueryManager")
    @patch("tools.persistence_store_builder.init_meetings_persistence_store")
    @patch("tools.persistence_store_builder.consts")
    @patch("builtins.open", new_callable=mock_open, read_data="SQL_DATA")
    def test_load_saved_data(self, mock_open, mock_consts, mock_init_meetings_persistence_store, mock_SqlQueryManager,
                             mock_vector_db_tool):
        # Arrange
        mock_query_manager = MagicMock()
        mock_SqlQueryManager.return_value.__enter__.return_value = mock_query_manager
        mock_consts.SQL_DATA_FILE_PATH = "fake_path.sql"
        mock_consts.VECTOR_DB_EMBEDDINGS_FILE_PATH = "fake_embeddings_path"

        # Act
        load_saved_data()

        # Assert
        mock_vector_db_tool.connect.assert_called_once()
        mock_vector_db_tool.disconnect.assert_called_once()
        mock_init_meetings_persistence_store.assert_called_once_with(mock_query_manager, auto_id_pk=False)
        mock_open.assert_called_once_with("fake_path.sql", "r")
        mock_query_manager.execute.assert_called_once_with("SQL_DATA")
        mock_vector_db_tool.load_meeting_summaries_embeddings.assert_called_once_with("fake_embeddings_path")

    @patch("tools.persistence_store_builder.vector_db_tool")
    @patch("tools.persistence_store_builder.SqlQueryManager")
    @patch("tools.persistence_store_builder.init_meetings_persistence_store")
    @patch("tools.persistence_store_builder.insert_meetings")
    @patch("tools.persistence_store_builder.insert_meeting_subjects")
    @patch("tools.persistence_store_builder.create_meeting_summaries")
    @patch("tools.persistence_store_builder.insert_meeting_summaries")
    @patch("tools.persistence_store_builder.logger")
    def test_build_meetings_persistence_store(self, mock_logger, mock_insert_meeting_summaries,
                                              mock_create_meeting_summaries, mock_insert_meeting_subjects,
                                              mock_insert_meetings, mock_init_meetings_persistence_store,
                                              mock_SqlQueryManager, mock_vector_db_tool):
        # Arrange
        meetings = [{"number": 1}, {"number": 2}]
        mock_query_manager = MagicMock()
        mock_SqlQueryManager.return_value.__enter__.return_value = mock_query_manager
        mock_create_meeting_summaries.return_value = [("speaker1", "summary1"), ("speaker2", "summary2")]
        mock_vector_db_tool.insert_meeting_summary.side_effect = [1, 2, 3, MilvusException("Error")]

        # Act
        build_meetings_persistence_store(meetings)

        # Assert
        mock_vector_db_tool.connect.assert_called_once()
        mock_vector_db_tool.disconnect.assert_called_once()
        mock_init_meetings_persistence_store.assert_called_once_with(mock_query_manager)
        mock_insert_meetings.assert_called_once_with(meetings, mock_query_manager)
        mock_insert_meeting_subjects.assert_called_once_with(meetings, mock_query_manager)
        mock_create_meeting_summaries.assert_any_call(meetings[0])
        mock_create_meeting_summaries.assert_any_call(meetings[1])
        mock_vector_db_tool.insert_meeting_summary.assert_any_call("summary1")
        mock_vector_db_tool.insert_meeting_summary.assert_any_call("summary2")
        mock_insert_meeting_summaries.assert_called_once_with([(1, "summary1", 1, "speaker1")], mock_query_manager)
        mock_insert_meeting_summaries.assert_called_once_with([(1, "summary1", 1, "speaker1")], mock_query_manager)
        mock_logger.info.assert_any_call("Processing meeting 1 ...")
        mock_logger.info.assert_any_call("Processing meeting 2 ...")
        mock_logger.info.assert_any_call("2 summaries created.")
        mock_logger.info.assert_any_call("2 summaries created.")


if __name__ == "__main__":
    unittest.main()