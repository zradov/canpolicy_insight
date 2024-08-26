import unittest
from unittest.mock import mock_open, patch
import json
from tools.meetings_tools import (
    load_meetings,
    get_meeting_docs,
    get_meeting_docs_per_person,
    create_meeting_summaries
)

class TestMeetingTools(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data='[{"meeting_id": 1, "summary": "Summary1"}, {"meeting_id": 2, "summary": "Summary2"}]')
    @patch('json.load')
    def test_load_meetings(self, mock_json_load, mock_open):
        # Mock the return value of json.load
        mock_json_load.return_value = [
            {"meeting_id": 1, "summary": "Summary1"},
            {"meeting_id": 2, "summary": "Summary2"}
        ]

        # Call the function
        meetings = load_meetings("dummy_path")

        # Expected meetings
        expected_meetings = [
            {"meeting_id": 1, "summary": "Summary1"},
            {"meeting_id": 2, "summary": "Summary2"}
        ]

        # Assertions
        self.assertEqual(meetings, expected_meetings)
        mock_open.assert_called_once_with("dummy_path", encoding="utf8")
        mock_json_load.assert_called_once()

    def test_get_meeting_docs(self):
        # Sample input
        meeting = {
            "interventions": [
                {
                    "person_speaking": "Person1",
                    "text_lines": ["Line 1.", "Line 2."]
                },
                {
                    "person_speaking": "Person2",
                    "text_lines": ["Line 3.", "Line 4."]
                }
            ]
        }

        # Expected output
        expected_documents = [
            {
                "speaker": "Person1",
                "text": "Line 1.Line 2."
            },
            {
                "speaker": "Person2",
                "text": "Line 3.Line 4."
            }
        ]

        # Call the function
        documents = get_meeting_docs(meeting)

        # Assertions
        self.assertEqual(documents, expected_documents)

    def test_get_meeting_docs_with_empty_interventions(self):
        # Sample input with empty interventions
        meeting = {
            "interventions": []
        }

        # Expected output
        expected_documents = []

        # Call the function
        documents = get_meeting_docs(meeting)

        # Assertions
        self.assertEqual(documents, expected_documents)

    def test_get_meeting_docs_with_no_text_lines(self):
        # Sample input with no text lines
        meeting = {
            "interventions": [
                {
                    "person_speaking": "Person1",
                    "text_lines": []
                }
            ]
        }

        # Expected output
        expected_documents = [
            {
                "speaker": "Person1",
                "text": ""
            }
        ]

        # Call the function
        documents = get_meeting_docs(meeting)

        # Assertions
        self.assertEqual(documents, expected_documents)

    def test_get_meeting_docs_per_person(self):
        # Sample input
        meeting = {
            "interventions": [
                {
                    "person_speaking": "Person1",
                    "text_lines": ["Line 1.", "Line 2."]
                },
                {
                    "person_speaking": "Person2",
                    "text_lines": ["Line 3.", "Line 4."]
                },
                {
                    "person_speaking": "Person3",
                    "text_lines": ["Line 5."]
                }
            ]
        }

        # Expected output
        expected_documents = {
            "Person1": ["Line 1.", "Line 2."],
            "Person2": ["Line 3.", "Line 4."],
            "Person3": ["Line 5."],
        }

        # Call the function
        documents = get_meeting_docs_per_person(meeting)

        # Assertions
        self.assertEqual(documents, expected_documents)

    def test_get_meeting_docs_per_person_empty_interventions(self):
        # Sample input with empty interventions
        meeting = {
            "interventions": []
        }

        # Expected output
        expected_documents = {}

        # Call the function
        documents = get_meeting_docs_per_person(meeting)

        # Assertions
        self.assertEqual(documents, expected_documents)

    def test_get_meeting_docs_per_person_with_no_text_lines(self):
        # Sample input with no text lines
        meeting = {
            "interventions": [
                {
                    "person_speaking": "Person1",
                    "text_lines": []
                }
            ]
        }

        # Expected output
        expected_documents = {
            "Person1": []
        }

        # Call the function
        documents = get_meeting_docs_per_person(meeting)

        # Assertions
        self.assertEqual(documents, expected_documents)

    @patch('tools.meetings_tools.get_meeting_docs_per_person')
    @patch('tools.meetings_tools.SummarizationTool')
    def test_create_meeting_summaries(self, MockSummarizationTool, mock_get_meeting_docs_per_person):
        # Mock the return value of get_meeting_docs_per_person
        mock_get_meeting_docs_per_person.return_value = {
            "Person1": ["Line 1.", "Line 2."],
            "Person2": ["Line 3."]
        }

        # Mock the SummarizationTool instance
        mock_summarization_tool_instance = MockSummarizationTool.return_value
        mock_summarization_tool_instance.run.side_effect = [
            ["Summary of Person1's speech."],
            ["Summary of Person2's speech."]
        ]
        mock_summarization_tool_instance.total_input_tokens_count = 100

        # Sample input
        meeting = {
            "interventions": [
                {
                    "person_speaking": "Person1",
                    "text_lines": ["Line 1.", "Line 2."]
                },
                {
                    "person_speaking": "Person2",
                    "text_lines": ["Line 3."]
                }
            ]
        }

        # Expected output
        expected_summaries = [
            ("Person1", "Summary of Person1's speech."),
            ("Person2", "Summary of Person2's speech.")
        ]

        # Call the function
        summaries = create_meeting_summaries(meeting)

        # Assertions
        self.assertEqual(summaries, expected_summaries)
        mock_get_meeting_docs_per_person.assert_called_once_with(meeting)
        self.assertEqual(mock_summarization_tool_instance.run.call_count, 2)
        self.assertEqual(mock_summarization_tool_instance.total_input_tokens_count, 100)

if __name__ == '__main__':
    unittest.main()