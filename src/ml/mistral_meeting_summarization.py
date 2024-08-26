import os
import consts
from ctransformers import AutoModelForCausalLM, AutoConfig
from tools.meetings_tools import load_meetings, get_meeting_docs


def get_summary_prompt_template():
    template = """<s>[INST] The following is a part of a transcript:
        {}
        Based on this, please identify the main points.
    [/INST] </s>"""

    return template


def get_reduced_summary_prompt_template():
    template = """<s>[INST] The following is set of summaries from the transcript:
        {doc_summaries}
        Take these and distill it into a final, consolidated summary of the main points.
        Construct it as a well organized summary of the main points and should be between 3 and 5 paragraphs.
        Answer:  [/INST] </s>"""

    return template


if __name__ == "__main__":
    print(os.path.join(consts.ML_MODELS_DOWNLOAD_DIR, "mistral-7b-instruct-v0.1.Q4_K_M.gguf"))
    llm = AutoModelForCausalLM.from_pretrained("TheBloke/Mistral-7B-Instruct-v0.1-GGUF",
                                               model_file=os.path.join(consts.ML_MODELS_DOWNLOAD_DIR,
                                                                       "mistral-7b-instruct-v0.1.Q4_K_M.gguf"),
                                               model_type="mistral",
                                               max_new_tokens=4096,
                                               context_length=30000,
                                               threads=os.cpu_count())
    meetings = load_meetings(consts.MEETINGS_DATA_FILE_PATH)
    meeting = [m for m in meetings if m["number"] == 133][0]
    meeting_docs = get_meeting_docs(meeting)
    meeting_text = os.linesep.join([f"{d['speaker']}: {d['text']}" for d in meeting_docs])
    summary_prompt = get_summary_prompt_template().format(meeting_text)
    output = llm(summary_prompt, max_new_tokens=4096, temperature=0.7, threads=10)
    print(summary_prompt)
    print("Text summary:")
    print(output)
