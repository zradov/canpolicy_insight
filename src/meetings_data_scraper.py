import re
import json
import consts
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin


class PageNotFoundException(BaseException):
    pass


def parse_url(url: str, parser_features: str | list[str] = "html.parser"):
    if parser_features is None:
        parser_features = []
    page = requests.get(url)
    if page.status_code > 400:
        raise PageNotFoundException(f"Failed to fetch page content for url '{url}'")
    soup = BeautifulSoup(page.text, features=parser_features)

    return soup


def get_meeting_date(div_meeting):
    meetings_css_class = [i for i in div_meeting.attrs["class"] if i.startswith("meeting-item-")][0]
    meeting_date = "-".join(meetings_css_class.split("-")[-3:])

    return meeting_date


def get_meeting_num(div_meeting):
    meeting_num = int(div_meeting.select_one(".meeting-number").text.split(" ")[1])

    return meeting_num


def get_time(str_time: str) -> str:
    str_hours_minutes, str_time_of_the_day = str_time.split(" ")
    hours, minutes = [int(i) for i in str_hours_minutes.split(":")]
    if hours != 12 and str_time_of_the_day.startswith("p"):
        hours += 12

    return "{:02}:{:02}".format(hours, minutes)


def get_meeting_time(div_meeting):
    meeting_time_arr = div_meeting.select_one(".the-time").text.split("-")
    str_start_time = get_time(meeting_time_arr[0].strip())
    end_time_arr = meeting_time_arr[1].strip().split(" ")
    str_end_time = get_time(" ".join(end_time_arr[:2]))
    str_time_zone = end_time_arr[-1].strip("()")

    return str_start_time, str_end_time, str_time_zone


def get_meeting_subjects(div_meeting):
    studies_and_activities = [li.text.strip() for li in div_meeting.select(".current-study")]

    return studies_and_activities


def remove_special_character(text: str, regex_multiple_spaces: re.Pattern) -> str:
    filtered_text = "".join(map(chr, [b if b < 127 else ord(" ") for b in text.encode("utf-8")]))
    filtered_text = filtered_text.strip()
    filtered_text = regex_multiple_spaces.sub(" ", filtered_text)

    return filtered_text

def get_meeting_interventions(meeting_num: int, url_format: str, regex_multiple_spaces: re.Pattern) -> list[object]:
    url = url_format.format(meeting_num)
    evidence_page_parser = parse_url(url)
    evidence_xml_relative_url = evidence_page_parser.select_one("a[class*='btn-export-xml']").attrs["href"]
    evidence_xml_absolute_url = urljoin(url, evidence_xml_relative_url)
    evidence_xml_page_parser = parse_url(evidence_xml_absolute_url, parser_features="lxml")
    interventions = []
    xml_interventions = evidence_xml_page_parser.find_all("intervention")
    for xml_intervention in xml_interventions:
        intervention = {
            "id": xml_intervention.attrs["id"],
            "person_speaking": remove_special_character(xml_intervention.find("affiliation").text, regex_multiple_spaces),
            "text_lines": [" ".join([remove_special_character(word, regex_multiple_spaces)
                                     for word in p.text.strip().split(" ")])
                           for p in xml_intervention.find_all('paratext')]
        }
        interventions.append(intervention)

    return interventions


if __name__ == "__main__":
    Path(consts.DATA_DIR).mkdir(parents=True, exist_ok=True)
    meetings_page_parser = parse_url(consts.MEETINGS_URL)
    div_meetings = meetings_page_parser.select("div[class*='meeting-item-']")
    meetings = []
    regex_multiple_spaces = re.compile(r"\s+")
    for d in div_meetings:
        try:
            meeting_start_time, meeting_end_time, meeting_time_zone = get_meeting_time(d)
            meeting_num = get_meeting_num(d)
            meeting = {
                "date": get_meeting_date(d),
                "start_time": meeting_start_time,
                "end_time": meeting_end_time,
                "time_zone": meeting_time_zone,
                "subjects": get_meeting_subjects(d),
                "number": meeting_num,
                "interventions": get_meeting_interventions(meeting_num, consts.MEETING_EVIDENCE_URL_FORMAT, regex_multiple_spaces)
            }
            meetings.append(meeting)
        except PageNotFoundException as ex:
            print(ex)
    with open(consts.MEETINGS_DATA_FILE_PATH, mode="w", encoding="utf8") as fh:
        json_meetings = json.dumps(meetings)
        fh.write(json_meetings)
