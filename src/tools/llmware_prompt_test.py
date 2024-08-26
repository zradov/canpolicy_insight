import time
from llmware.prompts import Prompt



def bling_meets_llmware_hello_world(test_list, model_name):
    """ Simple inference loop that loads a model and runs through a series of test questions. """

    t0 = time.time()

    print(f"\n > Loading Model: {model_name}...")

    prompter = Prompt().load_model(model_name)

    t1 = time.time()
    print(f"\n > Model {model_name} load time: {t1 - t0} seconds")

    for i, entries in enumerate(test_list):
        print(f"\n{i + 1}. Query: {entries['query']}")

        # run the prompt
        output = prompter.prompt_main(entries["query"], context=entries["context"]
                                      , prompt_name="default_with_context", temperature=0.30)

        llm_response = output["llm_response"].strip("\n")
        print(f"LLM Response: {llm_response}")
        print(f"LLM Usage: {output['usage']}")

    t2 = time.time()
    print(f"\nTotal processing time: {t2 - t1} seconds")

    return 0


if __name__ == "__main__":
    test_list = [
        { "query": "What columns are selected in the following query?",
          "context": (
            "SELECT speaker, COUNT(*) FROM meeting_summaries WHERE meeting_number IN (SELECT DISTINCT (meeting_number), id) GROUP BY speaker ORDER BY COUNT(*) DESC"
          )
        },
        { "query": "What columns are selected in the following query?",
          "context": (
              "select count(distinct(t1.speaker)) from test.conversations_summary t1 join ("
              "SELECT distinct(meeting_number) FROM test.conversations_summary order by meeting_number desc limit 2) "
              "t2 on t1.meeting_number = t2.meeting_number order by t1.speaker desc"
          )
        }
    ]
    bling_meets_llmware_hello_world(test_list, "llmware/bling-1b-0.1")