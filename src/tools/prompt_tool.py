import re
import os
import enum
import json
import consts
import logging
import instructor
from typing import List
from abc import ABC, abstractmethod
from pydantic import Field, BaseModel
from ctransformers import AutoModelForCausalLM
from tools import vector_db_tool
from tools.openai_tools import get_open_ai_client
from tools.db_tools import SqlQueryManager, get_meeting_summaries


MISTRAL_MODEL_DOWNLOAD_PATH = os.path.join(consts.ML_MODELS_DOWNLOAD_DIR, "7B-Instruct-v0.3")
logger = logging.getLogger(__file__)

QUERY_PLANNER_PROMPT = f"""
You are a world class query planning algorithm capable of breaking apart questions into its dependency queries,
such that the answers can be used to inform the parent question.
1. The queries should be of three types, depending on what is searched: "MEETING", "SUMMARY", or "SUBJECT".
2. The SUMMARY and the SUBJECT tables reference the MEETING table through their 'meeting_number' column.   
3. Each query must have a columns property with a list of columns containing relevant data for the question asked.
4. Determine parameters to be passed to each query.
    4.1. For MEETING queries, determine an optional daterange if specified, with parameters 'min_date' and 'max_date'.
         Where applicable, return also a 'filter'-parameter, in the format of [{{'field':'value'}},..]. Filters can be applied to the following fields: 'number', a meeting ID, for an example {{'number':133}}, 'meeting_date', a date of the meeting, for an example {{'meeting_date':'2023-06-20'}}, 'start_time', a start time of the meeting, for an example {{'start_time': '09:00:00'}} and 'end_time', a meeting ending time, for an example {{'end_time': '19:00:00'}}
    4.2. For SUMMARY queries, determine an appropriate 'keywords'-parameter, which is a list of keywords that can be used for finding the speakers and the summaries.
         Where applicable, return also a 'filter'-parameter, in the format of [{{'field':'value'}},..]. Filters can be applied to the following fields: 'id', an ID of summary, for an example {{'id': 1}}, 'summary', a summary text, for an example {{'summary':'Some meeting summary'}}, 'speaker', a name of the specific speaker, 'meeting_number', an ID of a meeting, for an example {{'meeting_number':133}}         
    4.3. For SUBJECT queries, determine an appropriate 'keywords'-parameter, which is a list of keywords that can be used for searching through subject names. 
         Where applicable, return a 'filter'-parameter, in the format of [{{'field':'value'}},..]. Filters can be applied to the following fields: 'meeting_number', an ID of a meeting, for an example {{'meeting_number':133}} and 'name', a subject name, for an example {{'name':'Chapter 6'}} 
5. Referencing values in dependency queries should be done using the following format: {{'field': 'FIELD_NAME', 'value': 'DEPENDENCY_NUM.FIELD_FROM_DEPENDENCY_RESULT', for an example {{'field': 'meeting_number', 'value': '1.number'}}, where '1' in '1.number' represents the dependency id and the 'number' the column in the results produced by the dependency '1'.
6. Sorting order should be specify inside the parameters using the following format: 'sort': {{'field': 'FIELD_NAME', 'order': 'SORT_DIRECTION'}}
7. Merge all queries that utilize the same table into a single query.
8. Searching for what a certain speaker talked about in a meeting should be done using the SUMMARY query and the 'speaker' filter field. 
9. Searching for specific term that was mentioned in a meeting should be done through the 'summary' column in the SUMMARY table.
10. Do not answer the question, simply provide a correct compute graph with good specific questions to ask and relevant dependencies.
11. Before you call the function, think step by step to get a better understanding of the problem.
"""

PROMPT_TEMPLATE_JSON = """Given the following json data:
{results_json}
Answer the following question in plain English: {question}"""

PROMPT_TEMPLATE = """Given the following data:
{results}
Answer the following question in plain English: {question}"""


class QueryType(str, enum.Enum):
    """Enumeration representing the types of queries that can be made to the database."""
    
    MEETING_SEARCH = "MEETINGS"
    #CONVERSATION_SEARCH = "CONVERSATION"
    SUMMARY_SEARCH = "SUMMARIES"
    SUBJECT_SEARCH = "SUBJECTS"


class Query(BaseModel):
    """Class representing a single query in a query plan."""

    id: int = Field(..., description="Unique ID of the query")

    parameters: dict = Field(
        ..., description="The parameters to be passed to the query"
    )

    dependencies: List[int] = Field(
        ..., description="List of IDs of queries that this query depends on"
    )
    

    query_type: QueryType = Field(
        ..., description="The type of query, either a meeting, summary, or subject query"
    )

    def _add_order_by(self, query):
        if "sort" in self.parameters:
            order_by = self.parameters.get("sort")
            query += f" ORDER BY "
            if isinstance(order_by, list):
                order_by_str = [f"{o['field']} {o['order']}" for o in order_by]
                query += f"{",".join(order_by_str)}"
            else:
                query += f"{order_by['field']} {order_by['order']}"

        return query

    def _add_limit(self, query):
        if "limit" in self.parameters:
            query += f" LIMIT {self.parameters['limit']}"

        return query

    def _add_filter(self, query):
        # In case a keyword search is required on the summary table, use the vector db to filter
        # out summaries that have a high similarity to the searched keywords.
        if ("keywords" in self.parameters) and (self.query_type == QueryType.SUMMARY_SEARCH):
            pass
        if ("filter" in self.parameters) and (len(self.parameters["filter"]) > 0):
            if "WHERE" in query:
                query += f" AND "
            else:
                query += f" WHERE "
            for index, filter_field in enumerate(self.parameters["filter"]):
                if index > 0:
                    query += " AND "
                field_name = filter_field['field']
                query += f"{field_name}"
                field_value = filter_field["value"]
                if isinstance(field_value, int) or ("." not in field_value):
                    query += f" = '{field_value}'"
                else:
                    dependency_num, column = field_value.split(".")
                    if dependency_num.isdigit():
                        dependency_num = int(re.sub(r"\D+", "", dependency_num))
                        dependency_results = self.parameters["dependencies_results"][dependency_num]
                        if isinstance(dependency_results, list):
                            if len(dependency_results) > 0:
                                query += f" IN ({','.join([str(i[0]) for i in dependency_results])})"
                        else:
                            query += f" = '{dependency_results}'"
                    else:
                        query += f" = '{field_value}'"

        return query

    def _add_group_by(self, query):
        if "group_by" in self.parameters:
            query += f" GROUP BY {self.parameters['group_by']}"

        return query

    def _execute_query(self, query):
        with SqlQueryManager() as query_manager:
            query_manager.execute(query)
            results = query_manager.fetchall()
            return results

    def run(self) -> list[tuple]:
        columns = ["*"] if "columns" not in self.parameters else self.parameters.get("columns")
        query = f"SELECT {",".join(columns)} FROM"
        if ((self.query_type == QueryType.MEETING_SEARCH) or
            (self.query_type == QueryType.SUMMARY_SEARCH) or
            (self.query_type == QueryType.SUBJECT_SEARCH)):
            if self.query_type == QueryType.MEETING_SEARCH:
                query += " meetings"
                if "daterange" in self.parameters:
                    daterange = self.parameters.get("daterange")
                    if "min_date" in daterange and "max_date" in daterange:
                        query += f" WHERE meeting_date BETWEEN '{daterange.get('min_date')}' AND '{daterange.get('max_date')}'"
                    elif "min_date" in daterange:
                        query += f" WHERE meeting_date >= '{daterange.get('max_date')}'"
                    elif "max_date" in daterange:
                        query += f" WHERE meeting_date <= '{daterange.get('max_date')}'"
            elif self.query_type == QueryType.SUMMARY_SEARCH:
                query += f" meeting_summaries"
            elif self.query_type == QueryType.SUBJECT_SEARCH:
                query += f" meeting_subjects"
            query = self._add_filter(query)
            query = self._add_group_by(query)
            query = self._add_order_by(query)
            query = self._add_limit(query)

            logger.info(f"query: {query}")

            return self._execute_query(query)


class QueryPlan(BaseModel):
    """Container class representing a tree of queries to run against a database, to answer a user's question"""

    query_plan: List[Query] = Field(
        ..., description="The query plan representing the queries to run"
    )

    def execute(self):
        # Dict to store the results of each query
        results = {}

        # Execute each query in the plan based on their dependencies
        for index, query in enumerate(self.query_plan):
            print(f"Running query {query.id}: {query.query_type.name}")
            if not query.dependencies:
                results[index + 1] = query.run()
            else:
                query.parameters["dependencies_results"] = results
                results[index + 1] = query.run()

        return results


class PromptTool(ABC):
    def __init__(self, planning_model="gpt-4-0613", query_planner_prompt=QUERY_PLANNER_PROMPT, prompt_template=PROMPT_TEMPLATE):
        self.planning_model = planning_model
        self.query_planner_prompt = query_planner_prompt
        self.prompt_text =  prompt_template

    def _get_query_planner(self, question: str) -> QueryPlan:
        messages = [
            {"role": "system", "content": self.query_planner_prompt},
            {"role": "user", "content": f"Consider: {question}\n Generate the correct query plan."}
        ]
        client = get_open_ai_client()
        client = instructor.patch(client)
        plan = client.chat.completions.create(
            model=self.planning_model,
            response_model=QueryPlan,
            messages=messages,
            temperature=0,
            max_tokens=1000,
        )

        return plan

    def _format_query_results(self, results: dict[int, list], plan: QueryPlan) -> str:
        formatted_query_results = []
        for single_query_plan in plan.query_plan:
            query_result = results[single_query_plan.id]
            query_type_name = single_query_plan.query_type.lower()
            for index, col in enumerate(single_query_plan.parameters["columns"]):
                formatted_query_results.append(f"{query_type_name} {col}s: "
                                               f"{",".join([str(v[index]) for v in query_result])}")
        return "\n".join(formatted_query_results)

    def _query_results_to_json(self, results: dict[int, list], plan: QueryPlan) -> str:
        query_results_dump = {}
        for single_query_plan in plan.query_plan:
            query_result = results[single_query_plan.id]
            query_type_name = single_query_plan.query_type.lower()
            query_dump_items = query_results_dump.get(query_type_name, [])
            for query_result_item in query_result:
                print(f"query_result_item: {str(query_result_item)}")
                for name, value in zip(single_query_plan.parameters["columns"], query_result_item):
                    query_dump_items.append({name: value})
            query_results_dump[query_type_name] = query_dump_items
        json_dump = json.dumps(query_results_dump, indent=4, sort_keys=True, default=str)

        return json_dump

    def _query_planner(self, question: str, query_planner_prompt=QUERY_PLANNER_PROMPT) -> QueryPlan:
        PLANNING_MODEL = "gpt-4-0613"
        messages = [
            {"role": "system", "content": query_planner_prompt},
            {"role": "user", "content": f"Consider: {question}\n Generate the correct query plan."}
        ]
        client = get_open_ai_client()
        client = instructor.patch(client)
        plan = client.chat.completions.create(
            model=PLANNING_MODEL,
            response_model=QueryPlan,
            messages=messages,
            temperature=0,
            max_tokens=1000,
        )

        return plan

    @abstractmethod
    def _prompt_llm_model(self, prompt_text: str) -> str:
        pass

    def generate(self, question: str) -> str:
        try:
            plan = self._get_query_planner(question)
        except Exception as ex:
            logger.error(ex)
            return f"No results found for the question: {question}"
        print(plan.model_dump())
        query_plan_results = plan.execute()
        # If any of the subqueries fail to found results, return no results message.
        if not all([len(v) for v in query_plan_results.values()]):
            return f"No results found for the question: {question}"
        #query_plan_results_json = self._query_results_to_json(query_plan_results, plan)
        #prompt_text = self.prompt_text.format(results_json=query_plan_results_json, question=question)
        formatted_query_results = self._format_query_results(query_plan_results, plan)
        prompt_text = self.prompt_text.format(results=formatted_query_results, question=question)

        prompt_result = self._prompt_llm_model(prompt_text)

        return prompt_result


class OpenAIPrompt(PromptTool):
    def _prompt_llm_model(self, prompt_text: str) -> str:
        openai_client = get_open_ai_client()
        response = openai_client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": f"{prompt_text}"
                }
            ],
            model="gpt-4"
        )

        return response.choices[0].message.content


class MistralPrompt(PromptTool):
    def prompt_llm_model(self, prompt_text: str) -> str:
        llm = AutoModelForCausalLM.from_pretrained(consts.ML_MODELS_DOWNLOAD_DIR,
                                                   model_file="mistral-7b-instruct-v0.1.Q4_K_M.gguf",
                                                   model_type="mistral",
                                                   max_new_tokens=4096,
                                                   context_length=30000,
                                                   threads=os.cpu_count())
        output = llm(prompt_text, max_new_tokens=500, temperature=0.7, threads=8)

        return output



def query_planner(question: str, query_planner_prompt=QUERY_PLANNER_PROMPT) -> QueryPlan:
    PLANNING_MODEL = "gpt-4-0613"
    messages = [
        { "role": "system", "content": query_planner_prompt },
        { "role": "user", "content": f"Consider: {question}\n Generate the correct query plan." }
    ]
    client = get_open_ai_client()
    client = instructor.patch(client)
    plan = client.chat.completions.create(
        model=PLANNING_MODEL,
        response_model=QueryPlan,
        messages=messages,
        temperature=0,
        max_tokens=1000,
    )

    return plan


def _get_context(query: str, context_data: list[str], simple_context_max_words: int = 6) -> str:
    words_count = sum([len(s.split(" ")) for s in context_data])
    if words_count <= simple_context_max_words:
        f"""[<s> [INST] For the question: {query} 
            the answer is: {"".join(context_data)} 
            Reformulate the answer. [/INST] </s>"""
    else:
        ""
    return ""


def _get_prompt_text(query: str, vector_db_host_name="localhost") -> str:
    vector_db_tool.connect(vector_db_host_name)
    vector_search_results = vector_db_tool.search(query)
    vector_ids = vector_search_results.ids
    vector_db_tool.disconnect()
    with SqlQueryManager() as query_manager:
        summaries = get_meeting_summaries(vector_ids, query_manager)
        prompt_text = f"""<s>[INST] Given the following context:
        {''.join(summaries)}
        Answer the following question: {query} [/INST] </s>"""

        return prompt_text


if __name__ == "__main__":
    #question = "Show me the summary of last 3 meetings?"
    question = "What Ms. Karen Hogan spoke about in the last meeting?"
    #question = "How many different speaker were there in the last 2 meetings? Name all the different speakers."
    #question = "What subjects were covered in the last meeting?"
    #question = "Show me the most 5 frequent speakers in the meetings held between 10/01/2023 and 20/02/2023?"
    #question = "Show me a summary of all meetings in which global warming was mentioned?"
    #question = "Select all speakers in the last 4 meetings grouped by how many times they speaked in the meeting in the descending order?"
    #question = "What is the report 6 main subject?"
    openai_prompt = OpenAIPrompt()
    result = openai_prompt.generate(question)
    print(result)
    #text1 = _get_prompt_text(question)
    #text2 = _get_prompt_text("global warming")
    #i = 1