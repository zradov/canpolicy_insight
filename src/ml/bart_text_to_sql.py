from time import time
from transformers import pipeline


if __name__ == "__main__":
    sql_generator = pipeline("text2text-generation", model="SwastikM/bart-large-nl2sql")

    start_time = time()
    query_question_with_context = "sql_prompt: query = What was the duration of the 3rd meeting held this year? sql_context: CREATE TABLE `meetings` (`number` int(4) NOT NULL, `date` date NOT NULL, `start_time` time NOT NULL, `end_time` time NOT NULL, `time_zone` char(3) NOT NULL, PRIMARY KEY (`number`)); CREATE TABLE `meeting_conversations` ( `id` int(6) NOT NULL AUTO_INCREMENT, `vector_id` bigint(20) DEFAULT NULL, `content` text NOT NULL, `meeting_number` int(4) NOT NULL, `speaker` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`), KEY `conversations_meetings_fk` (`meeting_number`), CONSTRAINT `conversations_meetings_fk` FOREIGN KEY (`meeting_number`) REFERENCES `meetings` (`number`) ON DELETE CASCADE); CREATE TABLE `meeting_summaries` ( `id` int(6) NOT NULL AUTO_INCREMENT, `vector_id` bigint(20) DEFAULT NULL, `content` text NOT NULL, `meeting_number` int(4) NOT NULL, `speaker` varchar(50) DEFAULT NULL, `date` date NOT NULL, PRIMARY KEY (`id`), KEY `conversations_summaries_meetings_fk` (`meeting_number`), CONSTRAINT `conversations_summaries_meetings_fk` FOREIGN KEY (`meeting_number`) REFERENCES `meetings` (`number`) ON DELETE CASCADE); CREATE TABLE `meeting_subjects` ( `name` varchar(100) NOT NULL, `meeting_number` int(4) NOT NULL, `date` date NOT NULL, PRIMARY KEY (`name`,`meeting_number`), KEY `subjects_meeting_fk` (`meeting_number`), CONSTRAINT `subjects_meeting_fk` FOREIGN KEY (`meeting_number`) REFERENCES `meetings` (`number`) ON DELETE CASCADE)"

    generate_kwargs = {
        "do_sample": True,
        "temperature": 0.7,
        "max_new_tokens": 35,
    }

    sql = sql_generator(query_question_with_context, max_new_tokens=1024)[0]['generated_text']

    print(f"Elapsed time: {time() - start_time}s")
    print(sql)
