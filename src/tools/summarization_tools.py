import os
import consts
import queue
import logging
import multiprocessing as mp
from transformers import pipeline
from transformers import AutoTokenizer
from concurrent.futures import ProcessPoolExecutor

logger = logging.getLogger(__name__)

SUMMARIZATION_PIPELINE = None


def meeting_summarization_worker(text_chunks_queue, summarized_text_chunks_queue, min_tokens_count=100):
    logger = logging.getLogger(__name__)
    process_name = mp.current_process().name
    logger.info(f"{process_name} Running text summarization ...")
    tokenizer = AutoTokenizer.from_pretrained(consts.TOKENIZER_MODEL_NAME)
    global SUMMARIZATION_PIPELINE
    if SUMMARIZATION_PIPELINE is None:
        SUMMARIZATION_PIPELINE = pipeline("summarization",
                                          model=consts.SUMMARIZER_MODEL_NAME,
                                          tokenizer=consts.TOKENIZER_MODEL_NAME)
    try:
        while not text_chunks_queue.empty():
            index, text_chunk = text_chunks_queue.get(block=False, timeout=1)
            logger.info(f"{process_name} Tokenizing text chunk with index {index} ...")
            tokens_count = len(tokenizer.tokenize(text_chunk))
            logger.info(f"{process_name} Summarizing text chunk with index {index}, tokens count: {tokens_count} ...")
            summarized = SUMMARIZATION_PIPELINE(text_chunk,
                                                min_length=min(min_tokens_count, tokens_count),
                                                max_length=tokens_count)
            logger.debug(f"{process_name} summarized text length: {len(summarized)}.")
            summarized_text_chunks_queue.put((index, summarized[0]["summary_text"]), block=False, timeout=1)
            logger.info(f"Tokenization and summarization completed for text chunk {index}.")
            text_chunks_queue.task_done()
    except queue.Empty:
        logger.error(f"{process_name} Summarization worker - queue empty error")
    except Exception as ex:
        logger.error(f"{process_name} Summarization worker - {ex}")


class SummarizationTool:

    def __init__(self,
                 tokenizer_model_name: str = consts.TOKENIZER_MODEL_NAME,
                 # Maximum input length reduced to 1023 because of the issues with index out of range when
                 # running text summarization pipeline.
                 max_input_length: int = 1023,
                 max_parallel_processes: int = os.cpu_count()):
        self.max_input_length = max_input_length
        self.max_parallel_processes = max_parallel_processes
        self.total_input_tokens_count = 0
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_model_name)

    def _get_text_chunks(self, docs: list[str]):
        text_lines = []
        text_chunks = []
        total_tokens_count = 0
        for index, doc in enumerate(docs):
            temp_doc = doc if doc.endswith(".") else f"{doc}."
            entire_text = "".join(text_lines)
            tokens_count = len(self.tokenizer.tokenize(entire_text + temp_doc))
            if tokens_count >= self.max_input_length:
                text_chunks.append((index, entire_text))
                text_lines.clear()
            total_tokens_count += tokens_count
            text_lines.append(temp_doc)
        text_chunks.append((len(text_chunks) + 1, "".join(text_lines)))
        return total_tokens_count, text_chunks

    def run(self, docs: list[str]):
        process_name = mp.current_process().name
        with mp.Manager() as manager:
            text_chunks_queue = manager.Queue()
            summarized_text_chunks_queue = manager.Queue()
            log_messages_queue = manager.Queue()
            total_tokens_count, text_chunks = self._get_text_chunks(docs)
            self.total_input_tokens_count += total_tokens_count
            _ = [text_chunks_queue.put(t) for t in text_chunks]
            logger.info(f"{process_name} Text chunks queue size: {text_chunks_queue.qsize()}")
            for index, text_chunk in text_chunks:
                logger.debug(f"{process_name} Text chunk {index}: {text_chunk}")
            with ProcessPoolExecutor(max_workers=min(text_chunks_queue.qsize(),
                                                     self.max_parallel_processes),
                                     mp_context=mp.get_context("spawn")) as executor:
                executor.map(meeting_summarization_worker,
                             [text_chunks_queue]*self.max_parallel_processes,
                             [summarized_text_chunks_queue]*self.max_parallel_processes)
            text_chunks_queue.join()

            ordered_summaries = []
            logger.info(f"{process_name} Summarized text chunks count: {summarized_text_chunks_queue.qsize()}")
            while not summarized_text_chunks_queue.empty():
                index, summary = summarized_text_chunks_queue.get()
                ordered_summaries.append((index, summary))
            ordered_summaries = sorted(ordered_summaries)
            ordered_summaries = [s[1] for s in ordered_summaries]\

            while not log_messages_queue.empty():
                logger.info(log_messages_queue.get())

            return ordered_summaries
