import os
import consts
from tools.summarization_tools import SummarizationTool
from tools.meetings_tools import load_meetings, get_meeting_docs_per_person


if __name__ == "__main__":
    target_meeting_num = 133
    meetings = load_meetings(consts.MEETINGS_DATA_FILE_PATH)
    meeting = [m for m in meetings if m["number"] == target_meeting_num][0]
    meeting_docs = get_meeting_docs_per_person(meeting)
    summarization_tool = SummarizationTool(max_parallel_processes=4)
    for speaker, docs in meeting_docs.items():
        speaker_summary_lines = summarization_tool.run(docs)
        speaker_summary = "".join([l for l in speaker_summary_lines])
        print(f"{os.linesep}Total input tokens: {summarization_tool.total_input_tokens_count}{os.linesep}")
        final_docs = []
        """
        for meeting_num, summarized_docs in summarized_docs.items():
            summarized_docs_text = ".".join(summarized_docs)
            summarized = summarizer(summarized_doc, min_length=100, max_length=500)
            final_docs.append(summarized[0]["summary_text"])
        print(f"Original text: {final_docs[0]}")
        print(f"Summary: {final_docs[0]}")
        """