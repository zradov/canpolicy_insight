import torch
from transformers import AutoModel, AutoTokenizer
from transformers import pipeline


if __name__ == "__main__":
    query = """[SEP]Given the following answer: 4"
    on the question:
    How many meetings were held this year?
    Reformulate the answer in a polite way.
    """
    """
    tokenizer = AutoTokenizer.from_pretrained("distilbert/distilbert-base-uncased")
    model = AutoModel.from_pretrained("distilbert/distilbert-base-uncased", torch_dtype=torch.float16, attn_implementation="flash_attention_2")
    encoded_input = tokenizer(query, return_tensors='pt').to("cpu")
    model.to("cpu")
    output = model(**encoded_input)
    """
    summarizer = pipeline("summarization", model="distilbert/distilbert-base-uncased")
    output = summarizer(query)
    print(output)