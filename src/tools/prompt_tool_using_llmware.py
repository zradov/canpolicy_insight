from time import time
from llmware.prompts import Prompt
from llmware.resources import PromptState


def prompt_state(llm_model, prompts: list[dict]):
    prompter = Prompt(save_state=True)
    prompt_id = prompter.prompt_id
    print(f"Loading the model {llm_model} ...")
    prompter.load_model(llm_model, temperature=1.0, Sample=False)

    print(f"Sending prompts to {llm_model} ...")
    for i, prompt in enumerate(prompts):
        print(f" - {prompt['query']}")
        response = prompter.prompt_main(prompt["query"], context=prompt["context"], register_trx=True)
        print(f" - LLM Response: {response}")

    interaction_history = prompter.interaction_history
    print(f"Prompt interaction history now contains {len(interaction_history)} interactions.")

    print(f"Reconstructed Dialog")
    dialog_history = prompter.dialog_tracker

    for i, conversation_turn in enumerate(dialog_history):
        print(f" - {i} [user]: {conversation_turn['user']}")
        print(f" - {i} [bot]: {conversation_turn['bot']}")

    prompter.save_state()
    prompter.clear_history()

    interaction_history = prompter.interaction_history
    print(f"> Prompt history has been cleared")
    print(f"> Prompt Interaction History now contains {len(interaction_history)} interactions")

    prompter.load_state(prompt_id)
    interaction_history = prompter.interaction_history
    print(f"The previous prompt state has been re-loaded")
    print(f"Prompt interaction history now contains {len(interaction_history)} interactions")

    prompt_transaction_report = PromptState().generate_interaction_report([prompt_id])
    print(f"A prompt transaction report has been generated: {prompt_transaction_report}")

    return 0


if __name__ == "__main__":
    #model_name = "openhermes-mistral-7b-gguf"
    model_name = "llama-2-7b-chat-gguf"
    prompts = [
        {
            "query": "What challenges are facing the participants of the meeting?",
            "context": "In some cases, there has been funding overlap. I can't comment on how board members were appointed. I would have expected that to be factored into an appointment process. If any funding has been given to an individual or organization that is ineligible, I would expect the government to take action to recover it. If they do not plan to do so, I'd expect them to be clear and transparent with Canadians about why. They have agreed with my recommendation. Time will tell how they implement the recommendation.I think it's a fundamental rule that if you're involved in a public organization, you should not be seen to be personally benefiting from public funds. There were definitely at least two projects that we deemed ineligible that were cases in which a conflict of interest had been declared and the person continued to vote. The National Research Council will make sure they have that expertise as they evaluate, based on merit, what every project should receive. There are two things with the contribution agreement between the federal government and the foundation that really sets out how they should determine eligibility.Mr. Chair, I'm not sure I can respond at that granular level. I'll see if the team can do some research. We looked at the process the foundation had in place. At times there were conflicts of interest that were well managed, and we highlighted the 90 that were not. I think there was a breakdown in communication here about things that really were not eligible to be funded under the contribution agreement. I don't believe that organization received funding. There were two organizations that received ecosystem funding. We found that there were challenges in the act's being respected."
        }
    ]
    start_time = time()
    prompt_state(model_name, prompts)
    print(f"Elapsed time: {time()-start_time}")