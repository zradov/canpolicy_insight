import unittest
from unittest.mock import patch, MagicMock
import os
from tools.config import Config, DbConfig, MilvusConfig, OpenAIConfig

class TestConfig(unittest.TestCase):

    @patch('tools.config.dotenv_values')
    @patch('tools.config.os.getenv')
    def test_config_initialization(self, mock_getenv, mock_dotenv_values):
        # Mock the environment and dotenv values
        mock_getenv.return_value = 'test'
        mock_dotenv_values.return_value = {
            'DB_HOST': 'localhost',
            'DB_PORT': '5432',
            'MILVUS_HOST': 'milvus.local',
            'MILVUS_PORT': '19530',
            'OPENAI_API_KEY': 'test'
        }

        # Test DbConfig
        db_config = DbConfig()
        self.assertEqual(db_config.host, 'localhost')
        self.assertEqual(db_config.port, '5432')

        # Test MilvusConfig
        milvus_config = MilvusConfig()
        self.assertEqual(milvus_config.host, 'milvus.local')
        self.assertEqual(milvus_config.port, '19530')

        # Test OpenAIConfig
        openai_config = OpenAIConfig()
        self.assertEqual(openai_config.api_key, 'test')

    @patch('tools.config.dotenv_values')
    @patch('tools.config.os.getenv')
    def test_config_attribute_error(self, mock_getenv, mock_dotenv_values):
        # Mock the environment and dotenv values
        mock_getenv.return_value = 'test'
        mock_dotenv_values.return_value = {
            'DB_HOST': 'localhost'
        }

        # Test attribute error
        db_config = DbConfig()
        with self.assertRaises(AttributeError):
            _ = db_config.non_existent_attribute

if __name__ == '__main__':
    unittest.main()