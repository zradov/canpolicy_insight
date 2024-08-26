import io
import os
import re
import consts
from time import time
from collections import Counter
from langchain.chains.llm import LLMChain
from langchain.docstore.document import Document
from langchain_community.llms import CTransformers
from tools.meetings_tools import load_meetings
from langchain_core.prompts.prompt import PromptTemplate
from langchain.chains.combine_documents.stuff import StuffDocumentsChain
from langchain.chains.combine_documents.reduce import ReduceDocumentsChain
from langchain.chains.combine_documents.map_reduce import MapReduceDocumentsChain


def load_llm(ml_models_download_dir: str) -> object:
    config = {'max_new_tokens': 4096, 'temperature': 0.7, 'context_length': 4096}
    llm = CTransformers(model="TheBloke/Mistral-7B-Instruct-v0.1-GGUF",
                        model_file=os.path.join(ml_models_download_dir, "mistral-7b-instruct-v0.1.Q4_K_M.gguf"),
                        config=config,
                        threads=os.cpu_count())

    return llm


def get_prompt_template():
    map_template = """<s>[INST] The following is a part of a transcript:
        {docs}
        Based on this, please identify the main points.
        Answer:  [/INST] </s>"""
    map_prompt = PromptTemplate.from_template(map_template)

    return map_prompt


def get_reduce_prompt_template():
    reduce_template = """<s>[INST] The following is set of summaries from the transcript:
        {doc_summaries}
        Take these and distill it into a final, consolidated summary of the main points.
        Construct it as a well organized summary of the main points and should be between 3 and 5 paragraphs.
        Answer:  [/INST] </s>"""
    reduce_prompt = PromptTemplate.from_template(reduce_template)

    return reduce_prompt


if __name__ == "__main__":
    meetings = load_meetings(src.consts.MEETINGS_DATA_FILE_PATH)
    print(f"Total number of meetings: {len(meetings)}")
    meeting_133 = [m for m in meetings if m["number"] == 133][0]
    print(f"Meeting 133 total inverventions: {len(meeting_133['interventions'])}")
    meeting_133_tokens_per_intervention = []
    text_buffer = io.StringIO()
    tokens_per_speaker = {}
    person_speaking_pattern = re.compile(r"^(?P<name>([^(])+)")
    documents = []
    for intervention in meeting_133["interventions"]:
        person_speaking = person_speaking_pattern.match(intervention["person_speaking"]).group("name").strip()
        tokens = tokens_per_speaker.get(person_speaking, [])
        text_lines = [line for line in intervention["text_lines"]]
        tokens.extend(("".join(text_lines).split(" ")))
        tokens_per_speaker[person_speaking] = tokens
        document_text = f"{person_speaking}: {''.join(text_lines)}"
        new_doc = Document(page_content=document_text, metadata={"speaker": person_speaking})
        documents.append(new_doc)
    counter = Counter({k: len(v) for k, v in tokens_per_speaker.items()})
    print(counter)
    llm = load_llm(consts.ML_MODELS_DOWNLOAD_DIR)
    map_prompt = get_prompt_template()
    map_chain = LLMChain(llm=llm, prompt=map_prompt)
    reduce_prompt = get_reduce_prompt_template()
    reduce_chain = LLMChain(llm=llm, prompt=reduce_prompt)
    combine_documents_chain = StuffDocumentsChain(
        llm_chain=reduce_chain, document_variable_name="doc_summaries"
    )
    reduce_documents_chain = ReduceDocumentsChain(
        # This is final chain that is called.
        combine_documents_chain=combine_documents_chain,
        # If documents exceed context for `StuffDocumentsChain`
        collapse_documents_chain=combine_documents_chain,
        # The maximum number of tokens to group documents into.
        token_max=4000,
    )
    map_reduce_chain = MapReduceDocumentsChain(
        # Map chain
        llm_chain=map_chain,
        # Reduce chain
        reduce_documents_chain=reduce_documents_chain,
        # The variable name in the llm_chain to put the documents in
        document_variable_name="docs",
        # Return the results of the map steps in the output
        return_intermediate_steps=True,
    )
    start_time = time()
    result = map_reduce_chain.invoke(documents[:2], return_only_outputs=True)
    print(f"Time taken: {time() - start_time} seconds")
    print(result['output_text'])



