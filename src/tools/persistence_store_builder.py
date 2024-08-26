import consts
import logging
from tools import vector_db_tool
from tools.db_tools import (
    SqlQueryManager,
    init_db,
    insert_meetings,
    insert_meeting_subjects,
    insert_meeting_summaries
)
from pymilvus import MilvusException
from tools.meetings_tools import create_meeting_summaries


logger = logging.getLogger(__file__)
logging.basicConfig(filename=consts.MAIN_LOG_FILE_PATH, encoding="utf-8", level=logging.DEBUG, force=True)


def init_meetings_persistence_store(query_manager: SqlQueryManager, auto_id_pk: bool = True) -> None:
    vector_db_tool.init_vectors_store(auto_id_pk)
    init_db(query_manager)


def load_saved_data():
    try:
        vector_db_tool.connect()
        with SqlQueryManager() as query_manager:
            init_meetings_persistence_store(query_manager, auto_id_pk=False)
            with open(consts.SQL_DATA_FILE_PATH, "r") as fp:
                data = fp.read()
                query_manager.execute(data)
            vector_db_tool.load_meeting_summaries_embeddings(consts.VECTOR_DB_EMBEDDINGS_FILE_PATH)
    finally:
        vector_db_tool.disconnect()


def build_meetings_persistence_store(meetings: list[dict]) -> None:
    try:
        vector_db_tool.connect()
        with SqlQueryManager() as query_manager:
            init_meetings_persistence_store(query_manager)
            insert_meetings(meetings, query_manager)
            insert_meeting_subjects(meetings, query_manager)
            for meeting in meetings:
                logger.info(f"Processing meeting {meeting['number']} ...")
                speakers_summaries = create_meeting_summaries(meeting)
                summary_data_to_insert = []
                logger.info(f"{len(speakers_summaries)} summaries created.")
                for speaker, summary in speakers_summaries:
                    try:
                        summary_vector_id = vector_db_tool.insert_meeting_summary(summary)
                        summary_data_to_insert.append(
                            (summary_vector_id, summary, meeting["number"], speaker)
                        )
                    except MilvusException as ex:
                        print(f"Failed to insert summary for {speaker} into the vector DB.")
                insert_meeting_summaries(summary_data_to_insert, query_manager)
    finally:
        vector_db_tool.disconnect()


if __name__ == "__main__":
    # Uncomment the following block of code to build the persistence store from the meetings data.
    from meetings_tools import load_meetings
    meetings = load_meetings(consts.MEETINGS_DATA_FILE_PATH)
    build_meetings_persistence_store(meetings)
    # Uncomment the following line to load the saved data into the persistence store.
    #load_saved_data()

