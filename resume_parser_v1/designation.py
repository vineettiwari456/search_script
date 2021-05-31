import pandas as pd
# removing extra char with spaces, displacing chars and numbers
from tqdm.notebook import trange, tqdm
from collections import defaultdict
import datetime, dateparser
import os, re

DAY_PATTERN = r'((0\d|1\d|2\d|3\d|\d)(\s{0,4})((st|nd|rd|th)(\s{0,4}of\s{0,4})?)?)'
MONTH_PATTERN = (
        r'(0\d|1\d|\d|january|february|march|april|may|june|july' +
        r'|august|september|october|november|december|jan|feb|mar|apr|may' +
        r'|jun|jul|aug|sep|sept|oct|nov|dec)'
)
YEAR_PATTERN = r'(19\d\d|20\d\d)'
NOW = r'\b(present|till date|current|since|till now)\b'
DELIMITER = r'(?P<{pattern}>(\s{{{{0,4}}}})(—|-|,|\.|/|\||\\|\s|’|\'|to)(\s{{{{0,4}}}}))'
DELIMITER_REPEAT = r'(?P={pattern})'
NUMBER_PATTERN = (
        r'(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen' +
        r'|([0-9]*[.])?[0-9]+)\+?'
)
NUMBER_DATE_PATTERN = r'(year|month|yrs)'

DATE_PATTERN_D_M_Y = r'{day}' + DELIMITER.format(pattern='pattern1') + r'{month}' + DELIMITER.format(
    pattern='pattern11') + r'{year}'
DATE_PATTERN_M_D_Y = r'{month}' + DELIMITER.format(pattern='pattern2') + r'{day}' + DELIMITER.format(
    pattern='pattern22') + r'{year}'
DATE_PATTERN_Y_M_D = r'{year}' + DELIMITER.format(pattern='pattern3') + r'{month}' + DELIMITER.format(
    pattern='pattern33') + r'{day}'
DATE_PATTERN_M_Y = r'{month}' + DELIMITER.format(pattern='pattern4') + r'{year}'
DATE_PATTERN_Y_M = r'{year}' + DELIMITER.format(pattern='pattern5') + r'{month}'
DATE_PATTERN_Y_Y = r'({year}|' + NOW + ')' + DELIMITER.format(pattern='pattern6') + r'({year}|' + NOW + ')'
DATE_PATTERN_YM_YM = r'(({year}' + DELIMITER.format(pattern='pattern71') + r'{month})|' + NOW + ')' + DELIMITER.format(
    pattern='pattern72') + r'(({year}' + DELIMITER.format(pattern='pattern73') + r'{month})|' + NOW + ')'
DATE_PATTERN_MY_MY = r'(({month}' + DELIMITER.format(pattern='pattern81') + r'{year})|' + NOW + ')' + DELIMITER.format(
    pattern='pattern82') + r'(({month}' + DELIMITER.format(pattern='pattern83') + r'{year})|' + NOW + ')'
NUMBER_DATE = NUMBER_PATTERN + DELIMITER.format(pattern='pattern9') + NUMBER_DATE_PATTERN
NUMBER_DATE_2 = NUMBER_PATTERN + NUMBER_DATE_PATTERN

DATE_PATTERN = (
        '(' +
        DATE_PATTERN_YM_YM + r'|' +
        DATE_PATTERN_MY_MY + r'|' +
        DATE_PATTERN_D_M_Y + r'|' +
        DATE_PATTERN_M_D_Y + r'|' +
        DATE_PATTERN_Y_M_D + r'|' +
        DATE_PATTERN_Y_Y + r'|' +
        DATE_PATTERN_M_Y + r'|' +
        DATE_PATTERN_Y_M + r'|' +
        NOW + r'|' +
        NUMBER_DATE + r'|' +
        NUMBER_DATE_2 +
        ')'
)
DATE_PATTERN = DATE_PATTERN.format(
    day=DAY_PATTERN,
    month=MONTH_PATTERN,
    year=YEAR_PATTERN,
)
DATES_RE = re.compile(DATE_PATTERN, re.IGNORECASE)

couple_date_pattern = ["pattern6", "pattern72", "pattern82"]
COUPLE_DELI = "$$$$"


class ExtractDesignation:
    def __init__(self):
        designation_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "designations.csv")
        designation_list = pd.read_csv(designation_file_path, header=None)
        des_lower = [i.lower() for i in designation_list[0]]
        self.designation_set_ngram = self.get_matching_set_ngram(list(set(des_lower)))
        # self.designation_set_ngram = designation_set_ngram
        self.original_form_dict = {}
        for i in designation_list[0]:
            self.original_form_dict[i.lower()] = i

    def get_matching_set_ngram(self, matching_list, ):
        # matching_ngrams = defaultdict(lambda: set())
        matching_ngrams = defaultdict(set)
        for matching_item in matching_list:
            k = len(matching_item.split())
            if k <= 5:
                matching_ngrams[k].add(" ".join(matching_item.split()).strip())
        return matching_ngrams

    def get_super_strings(self, list_of_strings):
        sub_strings = []
        for st in list_of_strings:
            for st1 in list_of_strings:
                if st in st1 and st != st1:
                    sub_strings.append(st)
        return [s for s in list_of_strings if s not in sub_strings]

    def get_ngrams(self, text, n):
        ngrams = [" ".join(b) for l in text for b in zip(*self.get_ngram_list(l, n))]
        return ngrams

    def get_ngram_list(self, text, n):
        ngram_list = []
        n -= 1
        text = " ".join([t for t in text.split(" ") if t != ""])
        for i in range(n + 1):
            if i - n != 0:
                ngram_list.append(text.split(" ")[i:i - n])
            else:
                ngram_list.append(text.split(" ")[i:])
        return ngram_list

    def get_super_strings(self, list_of_strings):
        sub_strings = []
        for st in list_of_strings:
            for st1 in list_of_strings:
                if st in st1 and st != st1:
                    sub_strings.append(st)
        return [s for s in list_of_strings if s not in sub_strings]

    def match(self, text, matching_set_ngram):
        list_of_matches = set()
        for n, matching_set in matching_set_ngram.items():
            tokens = self.get_ngrams(text, n)
            for token in tokens:
                if token.lower() in matching_set:
                    list_of_matches.add(token)
        super_strings = self.get_super_strings(list_of_matches)
        return super_strings

    def add_space(self, text):
        if text == "":
            return ""
        new_text = "" + text[0]
        for i in range(1, len(text)):
            if (text[i].isalpha() and text[i - 1].isnumeric()) | (text[i].isnumeric() and text[i - 1].isalpha()):
                new_text += " " + text[i]
            else:
                new_text += text[i]
        return new_text

    def clean_text(self, text):
        text = "".join([c if (c.isalnum() or c == "+" or c == "#") else " " for c in text])
        text = self.add_space(text)
        text = " ".join([t for t in text.split(" ") if t != ""])
        print(text[:300])
        return text[:300]

    def get_context_lines(self, i, text):
        if i == 0 and len(text) == 1:
            return [[], [text[0], '']]
        elif i == 0 and len(text) > 2:
            return [[], [text[1], text[2]]]
        elif i == 1 and len(text) > 3:
            return [[text[0]], [text[2], text[3]]]
        elif i == len(text) - 1:

            return [[text[len(text) - 3], text[len(text) - 2]], []]
        elif i == len(text) - 2:
            return [[text[len(text) - 4], text[len(text) - 3]], [text[i + 1]]]
        else:
            return [[text[i - 2], text[i - 1]], [text[i + 1], text[i + 2]]]

    def get_context_around(self, text, matched_text):
        context = []
        i = 0
        for line in text:
            for match in matched_text:
                if match in line:
                    # print(i, match, get_context_lines(i, text))
                    # print("\n")
                    match_context = self.get_context_lines(i, text) + [line.split(match, 1)] + [i / len(text)] + [match]
                    context.append(match_context)
                    # i+=1
                    # break
            i += 1
        return context

    def open_context(self, con):
        if len(con) == 1:
            con = [""] + con
        elif len(con) == 0:
            con = ["", ""]
        return con

    def explode_context(self, context):
        top_context, down_context, horizontal_context, pos, designation = context
        # print(top_context, down_context)
        return self.open_context(top_context) + self.open_context(down_context) + horizontal_context + [pos] + [
            designation]

    def explode_(self, context):
        con_ = []
        for c in context:
            con_.append(self.explode_context(c))
        return con_

    def get_couple_del(self, dic):
        for pat in couple_date_pattern:
            if pat in dic and dic[pat] != None:
                return pat, dic[pat]

    def get_dates(self, text):
        dates_list = []
        try:
            for i in DATES_RE.finditer(text.replace(',', ' ')):
                couple_del = self.get_couple_del(i.groupdict())
                if couple_del != None:
                    pat, deli = couple_del
                    ind = i.start(pat)
                    t = text[i.start():ind] + COUPLE_DELI + text[ind + len(deli):i.end()]
                    index = len(i.group())
                    dates_list.append(t)
                else:
                    dates_list.append(i.group())
            return dates_list
        except Exception as e:
            # print("get_dates+++++",e,text)
            return []

    def get_dateTime(self, date):
        try:
            if date.lower() in [i for i in re.split('\||\\\\b|\(|\)', NOW) if i != ""]:
                return datetime.datetime.today()
            else:
                date = date.replace("’", " ").replace("'", " ")
                return dateparser.parse(date)
        except Exception as e:
            # print("get_dateTime++++++",e, date)
            pass

    def get_dates_in_data(self, data, disable=False):
        dates_all = []
        for index, row in tqdm(data.iterrows(), disable=disable):
            context = row["top_context_2"] + " @#$ " + \
                      row["top_context_1"] + " @#$ " + \
                      row["horizontal_context_left"] + " @#$ " + row["horizontal_context_right"] + " @#$ " + \
                      row["down_context_2"] + " @#$ " + \
                      row["down_context_1"]
            # context = row["horizontal_context_left"] + " @#$ " +  row["horizontal_context_right"]

            dates = self.get_dates(context)
            if len(dates) > 0:
                dates = [d.split("$$$$") for d in dates]
                dates = [item for sublist in dates for item in sublist]
                dates = [self.get_dateTime(d) for d in dates]
                dates = [d for d in dates if d != None]
                dates.sort()
                if len(dates) > 0:
                    dates_all.append(dates[-1])
                else:
                    dates_all.append(None)
            else:
                dates_all.append(None)
        data["dates"] = dates_all

        return data

    def append_largest_date(self, training_data):
        resume_ids = list(set(training_data["Resume ID"]))
        largest_date = []
        for id_ in resume_ids:
            res_data = training_data[training_data["Resume ID"] == id_]
            dates = list(res_data["dates"])
            dates = [d for d in dates if not pd.isnull(d)]
            dates.sort()
            if len(dates) > 0:
                largest_date.append([id_, dates[-1]])
            else:
                largest_date.append([id_, None])
        dates = pd.DataFrame(largest_date, columns=["Resume ID", "current_date"])
        training_data = pd.merge(training_data, dates, on='Resume ID')
        return training_data

    def get_is_current_date(self, training_data, disable=False):
        dates_all = []
        for index, row in tqdm(training_data.iterrows(), disable=disable):
            if row["dates"] == None:
                dates_all.append(0)
            elif row["dates"].date() == row["current_date"].date():
                dates_all.append(1)
            else:
                dates_all.append(0)
        training_data["is_current_date"] = dates_all
        return training_data

    def get_designaiton(self, text, clean=True):
        try:
            text = text[:50]
            if clean:
                text = [self.clean_text(t) for t in text]
            matched_text = self.match(text, self.designation_set_ngram)
            context = self.get_context_around(text, matched_text)
            data = self.explode_(context)

            data = pd.DataFrame(data, columns=["top_context_2", "top_context_1", "down_context_2", "down_context_1",
                                               "horizontal_context_left", "horizontal_context_right", "position",
                                               "designation"])

            data = self.get_dates_in_data(data, disable=True)

            data["Resume ID"] = 1
            data = self.append_largest_date(data)
            data = self.get_is_current_date(data, disable=True)
            if len(data[data["is_current_date"] == 1]) > 0:
                return self.original_form_dict[data[data["is_current_date"] == 1]["designation"].iloc[0].lower()]
            elif len(data[data["is_current_date"] == 0]) > 0:
                return self.original_form_dict[data[data["is_current_date"] == 0]["designation"].iloc[0].lower()]
            return ""
        except Exception as ex:
            print(ex)
            return ''


if __name__ == "__main__":
    obj = ExtractDesignation()
    text = ['AMIT KUMAR MISHRA',
            '1-10-2017 senior software engineer I intend to be a part of an organization where I can constantly learn and develop my technical skills and',
            'make best use of it.', 'E-Mail ID: mishra1.amit29@gmail.com',
            'Mobile/Phone: +91-8583885211                                                    Date of Birth: 29/11/1995',
            'ACADEMICS',
            'Qualification                 Institute                 Board/University             Year            %/CGPA',
            ' Graduation       RCC Institute of Information',
            '  (B. Tech,          Technology, Kolkata                   MAKAUT, WB                2019              7.17',
            '    ECE)',
            "     XII             St. Xavier's Institution,               ISC,CISCE               2015             85.25%",
            '                         Panihati, Kolkata',
            "      X              St. Xavier's Institution,              ICSE,CISCE               2012             83.40%",
            '                         Panihati, Kolkata', 'CERTIFICATES / PUBLICATIONS',
            'Certifications/Publications           Certificate for Completion of C Training (Spoken Tutorial) offered by Spoken',
            '                                      Tutorial Project, IIT Bombay, funded by National Mission on Education',
            '                                      through ICT, MHRD, Govt., of India.',
            '                                      Certificate for Completion of JAVA Training (Spoken Tutorial) offered by',
            '                                      Spoken Tutorial Project, IIT Bombay, funded by National Mission on Education',
            '                                      through ICT, MHRD, Govt., of India.',
            '                                      Certificate for Completion of PYTHON Training (Spoken Tutorial) offered by',
            '                                      Spoken Tutorial Project, IIT Bombay, funded by National Mission on Education',
            '                                      through ICT, MHRD, Govt., of India.',
            '                                      Certificate for Completion of CCNA Networking Training.',
            'WORK EXPERIENCE / INTERNSHIP PROJECT/TRAININGS', '  Organization/Company Name with',
            '                                                                Position & Job Responsibility',
            '                duration',
            '            Brillica Services                       CISCO Certified CCNA Switching & Routing Training V3.0 .',
            '4Weeks   (20th June `18 to 13th July `18)', '\x0cINTERNSHIP/ PROJECTS',
            'SL.NO.               PROJECT DOMAIN                DURATION                     PROJECT TASK',
            '     1.     Enterprise Network Security under      June \xad          Designing various Topologies using Switching',
            '            CISCO Routing & Switching              July`2018       & Routing Protocol',
            '            (Networking)',
            "     2.     IOT Based Irrigation System using      August'2018     Designing various Techniques to measure",
            "            Arduino (Microcontroller &             \xad May'2019      various Irrigation Techniques using Arduino",
            '            Microprocessor, IOT)                                   Programming', 'KEY SKILLS',
            '1.               TECHNOLOGIES / TOOLS                       MS OFFICE, MATLAB',
            '2.               PROGRAMMING LANGUAGE                       BASICS OF C, JAVA & SQL',
            '3.               OPERATING SYSTEM                           WINDOWS 10, 8.1, 8, 7, XP & Ubuntu',
            'ACHIEVEMENTS',
            '          Got Cash Award of Rs. 1000 & Monthly Scholarship of Rs. 200 in the academic year 2012 on behalf',
            '          of Suburban Educational Society for my academic proficiency in I.C.S.E 2012 Examination for',
            '          period of April,2012 to September,2012',
            '          Got Cash Award of Rs.2500 in the academic year 2015 on behalf of Suburban Educational Society',
            '          for my academic proficiency in I.S.C 2015',
            '          Got Cash Award in TechFest in College in 4th Semester, 2017', 'EXTRA CURRICULAR ACTIVITIES',
            'Participation                      Participated in Robotics (Inter-college Fest)',
            '                                   Participated in Capgemini Tech Challenge.',
            '                                   Participated in Technical quiz organized by "The Institution of',
            '                                   Engineers".',
            "                                   Member of School's Eco-club and Social service Club.",
            '                                   Volunteer at "MAY-DAY" event at school.',
            'Career Interest                    Keen interest in Learning new Technologies.',
            '                                   Interest in learning Mobile Application Development.',
            '                                   Interest in Front and Backend Development on Web',
            'Hobbies                            Blog Writing(Mostly Travelling).',
            '                                   Volunteer Work',
            '                                   Music Playing & Listening',
            '                                   Sports: Badminton, Cricket, Bicycling',
            '                                   Teaching Children', '\x0cSOFT SKILLS',
            '             TIME MANAGEMENT ABILITY                           SELF MOTIVATED',
            '             DECISION MAKING                                   EXTROVERT & AGILE',
            '             ADAPTIBLITY                                       ETHICS & MATH SKILLS', 'LANGUAGES KNOWN',
            '        LANGUAGES                  Read                       Write                     Speak',
            '          English                  YES                        YES                       YES',
            '           Hindi                   YES                        YES                       YES',
            '          Bengali                  YES                        YES                       YES',
            '          Bhojpuri                 YES                        YES                       YES',
            '         I hereby declare that the above information is true and correct to the best of my knowledge.',
            '         20/07/2019', 'Date:                                                Signature:', '\x0c']
    import time

    st = time.time()
    print("+++++", obj.get_designaiton(text))
    print(time.time() - st)
