import os
import nltk
import consts
from transformers import pipeline
from nltk.tokenize import word_tokenize
from tools.meetings_tools import load_meetings, get_meeting_docs


MEETING_NUM = 133


# Semantic Coherence (Example implementation)
def semantic_coherence(generated_summary, dialogue):
    # Your semantic coherence metric calculation logic
    summary_tokens = word_tokenize(generated_summary.lower())
    dialogue_tokens = word_tokenize(dialogue.lower())

    # Calculate the intersection of tokens
    common_tokens = set(summary_tokens) & set(dialogue_tokens)

    # Calculate semantic coherence score based on the ratio of common tokens to summary length
    coherence_score = len(common_tokens) / len(summary_tokens)

    return coherence_score


# Factual Accuracy (Example implementation)
def factual_accuracy(generated_summary, reference_summary):
    # Your factual accuracy metric calculation logic
    gen_tokens = set(word_tokenize(generated_summary.lower()))
    ref_tokens = set(word_tokenize(reference_summary.lower()))

    # Calculate the intersection of tokens
    common_tokens = gen_tokens & ref_tokens

    # Calculate factual accuracy score based on the ratio of common tokens to reference summary length
    accuracy_score = len(common_tokens) / len(ref_tokens) if len(ref_tokens) != 0 else 0
    return accuracy_score


# Content Coverage (Example implementation)
def content_coverage(generated_summary, dialogue):
    # Your content coverage metric calculation logic

    summary_tokens = set(word_tokenize(generated_summary.lower()))
    dialogue_tokens = set(word_tokenize(dialogue.lower()))

    # Calculate the intersection of tokens
    common_tokens = summary_tokens & dialogue_tokens

    # Calculate the content coverage score based on the ratio of common tokens to dialogue length
    coverage_score = len(common_tokens) / len(dialogue_tokens) if len(dialogue_tokens) != 0 else 0
    return coverage_score


def evaluate_model(model_name, dialogue, target_summary):
    print(f"Evaluating model: {model_name}")
    # Initialize the summarization pipeline
    summarizer = pipeline("summarization", model=model_name, tokenizer=model_name)

    generated_summary = summarizer(dialogue, max_length=100, min_length=30, do_sample=False)[0]["summary_text"]
    coherence_score = semantic_coherence(generated_summary, dialogue)
    accuracy_score = factual_accuracy(generated_summary, target_summary)
    coverage_score = content_coverage(generated_summary, dialogue)

    return coherence_score, accuracy_score, coverage_score


if __name__ == "__main__":
    nltk.download('punkt')
    bleu_scores = []
    models = [
        "facebook/bart-large-cnn",
        "t5-large",
        "sshleifer/distilbart-cnn-12-6",
        "google/pegasus-large",
        "allenai/led-large-16384-arxiv",
    ]
    meetings = load_meetings(consts.MEETINGS_DATA_FILE_PATH)
    meeting = [m for m in meetings if m["number"] == MEETING_NUM][0]
    meeting_docs = get_meeting_docs(meeting)
    dialog = meeting_docs[0]
    summary = "The House of Commons Standing Committee on Public Accounts is holding a meeting in a hybrid format, with members attending in person and possibly remotely using the Zoom application. The committee is resuming its study of report 6, Sustainable Development Technology Canada, from the 2024 reports 5 to 7 of the Auditor General of Canada. The committee is requesting that participants use approved black earpieces, keep them away from microphones, and place them face down on the table. The committee welcomes witnesses from the Office of the Auditor General, Sustainable Development Technology Canada, and Sustainable Development Technology Canada."
    evaluation_results_list = []
    for model_name in models:
        coherence_score, accuracy_score, coverage_score = evaluate_model(model_name, dialog, summary)
        print(f"Model {model_name} results:")
        print(f"Semantic coherence score: {coherence_score}")
        print(f"Factual accuracy score: {accuracy_score}")
        print(f"Content coverage score: {coverage_score}{os.linesep}")
