import os
import re
import json
import logging
from tools.summarization_tools import SummarizationTool

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def load_meetings(meetings_file_path: str) -> list[dict]:
    with open(meetings_file_path, encoding="utf8") as fh:
        meetings = json.load(fh)
        return meetings


def get_meeting_docs(meeting: dict) -> list[dict]:
    person_speaking_pattern = re.compile(r"^(?P<name>([^(])+)")
    documents = []
    for intervention in meeting["interventions"]:
        person_speaking = person_speaking_pattern.match(intervention["person_speaking"]).group("name").strip()
        text_lines = [line for line in intervention["text_lines"]]
        document_text = ''.join(text_lines)
        documents.append({
            "speaker": person_speaking,
            "text": document_text
        })

    return documents


def get_meeting_docs_per_person(meeting: dict) -> dict[str, list[str]]:
    person_speaking_pattern = re.compile(r"^(?P<name>([^(])+)")
    documents = {}
    for intervention in meeting["interventions"]:
        person_speaking = person_speaking_pattern.match(intervention["person_speaking"]).group("name").strip()
        text_lines = documents.get(person_speaking, [])
        text_lines.extend([line for line in intervention["text_lines"]])
        documents[person_speaking] = text_lines

    return documents


def create_meeting_summaries(meeting: dict) -> list[tuple[str, str]]:
    meeting_docs = get_meeting_docs_per_person(meeting)
    meeting_summaries = []
    summarization_tool = SummarizationTool(max_parallel_processes=4)
    for speaker, docs in meeting_docs.items():
        speaker_summary_lines = summarization_tool.run(docs)
        speaker_summary = "".join([l for l in speaker_summary_lines])
        logger.debug(f"{os.linesep}Total input tokens: {summarization_tool.total_input_tokens_count}{os.linesep}")
        meeting_summaries.append((speaker, speaker_summary))

    return meeting_summaries
