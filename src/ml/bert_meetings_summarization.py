import consts
from summarizer.sbert import SBertSummarizer
from tools.meetings_tools import load_meetings, get_meeting_docs


MEETING_NUM = 133


if __name__ == "__main__":
    model = SBertSummarizer('paraphrase-MiniLM-L6-v2')
    meetings = load_meetings(consts.MEETINGS_DATA_FILE_PATH)
    meeting = [m for m in meetings if m["number"] == MEETING_NUM][0]
    meeting_docs = get_meeting_docs(meeting)
    meeting_text = "\r\n".join([f"{d['speaker']}: {d['text']}" for d in meeting_docs])
    summary = model(f"{meeting_docs[0]['speaker']}: {meeting_docs[0]['text']}", num_sentences=10)
    print(summary)

