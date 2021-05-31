# -*-coding:utf-8 -*-

import dateparser
import datetime
import re

#removing extra char with spaces, displacing chars and numbers
def add_space(text):
    if text == "":
        return ""
    new_text = "" + text[0]
    for i in range(1,len(text)):
        if (text[i].isalpha() and text[i-1].isnumeric()) | (text[i].isnumeric() and text[i-1].isalpha()):
            new_text+=" "+text[i]
        else:
            new_text+=text[i]
    return new_text

def clean_text(text):
    text = "".join([c if (c.isalnum() or c == ".") else " " for c in text])
    list_of_exclusion = ["windows", "window", "ms office", "microsoft office", "word", "excel", "power point", "outlook", "microsoft"]
    for word in list_of_exclusion:
        if word in text.lower():
            return ""
    text = add_space(text)
    text = " ".join([t for t in text.split(" ") if t != ""])
    list_of_singular_exclusion = ["age", "old", "old.", "age."]
    for word in list_of_singular_exclusion:
        for t in text.split(" "):
            if word.lower()==t.lower():
                return ""
    return text


DAY_PATTERN = r'((0\d|1\d|2\d|3\d|\d)(\s{0,4})((st|nd|rd|th)(\s{0,4}of\s{0,4})?)?)'
MONTH_PATTERN = (
    r'(0\d|1\d|\d|january|february|march|april|may|june|july' +
    r'|august|september|october|november|december|jan|feb|mar|apr|may' +
    r'|jun|jul|aug|sep|sept|oct|nov|dec)'
)
YEAR_PATTERN = r'(19\d\d|20\d\d)'
NOW = r'present|till date|current|since|till now'
DELIMITER = r'(?P<{pattern}>(\s{{{{0,4}}}})(—|-|,|\.|/|\||\\|\s|’|\'|to)(\s{{{{0,4}}}}))'
DELIMITER_REPEAT = r'(?P={pattern})'
NUMBER_PATTERN = ( 
    r'(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen' +
    r'|([0-9]*[.])?[0-9]+)\+?'
)
NUMBER_DATE_PATTERN = r'(year|month|yrs)'

DATE_PATTERN_D_M_Y = r'{day}'+DELIMITER.format(pattern='pattern1')+r'{month}'+DELIMITER.format(pattern='pattern11')+r'{year}'
DATE_PATTERN_M_D_Y = r'{month}'+DELIMITER.format(pattern='pattern2')+r'{day}'+DELIMITER.format(pattern='pattern22')+r'{year}'
DATE_PATTERN_Y_M_D = r'{year}'+DELIMITER.format(pattern='pattern3')+r'{month}'+DELIMITER.format(pattern='pattern33')+r'{day}'
DATE_PATTERN_M_Y = r'{month}'+DELIMITER.format(pattern='pattern4')+r'{year}'
DATE_PATTERN_Y_M = r'{year}'+DELIMITER.format(pattern='pattern5')+r'{month}'
DATE_PATTERN_Y_Y = r'({year}|'+NOW+')'+DELIMITER.format(pattern='pattern6')+r'({year}|'+NOW+')'
DATE_PATTERN_YM_YM = r'(({year}'+DELIMITER.format(pattern='pattern71')+r'{month})|'+NOW+')' + DELIMITER.format(pattern='pattern72') + r'(({year}'+DELIMITER.format(pattern='pattern73')+r'{month})|'+NOW+')' 
DATE_PATTERN_MY_MY = r'(({month}'+DELIMITER.format(pattern='pattern81')+r'{year})|'+NOW+')' + DELIMITER.format(pattern='pattern82') + r'(({month}'+DELIMITER.format(pattern='pattern83')+r'{year})|'+NOW+')'
NUMBER_DATE = NUMBER_PATTERN+DELIMITER.format(pattern='pattern9')+NUMBER_DATE_PATTERN
NUMBER_DATE_2 = NUMBER_PATTERN+NUMBER_DATE_PATTERN

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




couple_date_pattern = ["pattern6","pattern72","pattern82"]
COUPLE_DELI = "$$$$"
def get_couple_del(dic):
    for pat in couple_date_pattern:
        if pat in dic and dic[pat] != None:
            return pat, dic[pat]

def get_dates(text):
    dates_list = []
    try:
        for i in DATES_RE.finditer(text.replace(',' ,' ')):
            couple_del = get_couple_del(i.groupdict())
            if couple_del!=None:
                pat, deli = couple_del
                ind = i.start(pat)
                t = text[i.start():ind] + COUPLE_DELI + text[ind+len(deli):i.end()]
                index = len(i.group())
                dates_list.append(t)
            else:
                dates_list.append(i.group())
        return dates_list
    except Exception as e:
        #print(e)
        return []

def get_dates_list(text_list):
    dates_list = []
    for text in text_list:
        dates = get_dates(text)
        if dates != []:
            dates_list.append(dates)
    return dates_list


years = ["one","two","three","four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen"]
number_dict = {}
i=1
for year in years:
    number_dict[year] = i
    i+=1

def get_num(num):
    string_num = str(num).strip().replace("+","")
    if string_num.isdigit():
        return int(string_num)
    elif string_num.replace(".","").isdigit():
        return float(string_num)
    else:
        return number_dict.get(num.strip(),-1)


def get_experience_block(text):
    i = 0
    exp = []
    ed = []
    for t in text:
        if 'experience' in t.lower():
            exp+=[i,]
        elif 'education' in t.lower() or 'academic' in t.lower() or "academia" in t.lower():
            ed+=[i,]
        i+=1
    exp = [('exp', e) for e in exp]
    ed = [('ed', e) for e in ed]
    add = exp + ed
    add = sorted(add, key=lambda x: x[1])
    x = -1
    y = -1
    go = -1
    for e, num in add:
        if e == "ed" and go!=0:
            x = num
            go = 0
        elif e == 'exp' and go==0:
            y = num
            break
    if x != -1 and y !=-1:
        return x,y
    elif x != -1 and y == -1:
        return x,len(text)
    else:
        return -1,-1




def parse_list(date_interval_list):
    parsed_interval_list = []
    for date_tup in date_interval_list:
        date_1 = dateparser.parse(date_tup[0])
        date_2 = dateparser.parse(date_tup[1])
        parsed_interval_list.append((min(date_1, date_2), max(date_1, date_2)))
    return parsed_interval_list

def get_exp(date_interval_list):
    n = len(date_interval_list)
    #date_interval_list = parse_list(date_interval_list)
    parsed_interval_list = []
    for date_tup in date_interval_list:
        parsed_interval_list.append((min(date_tup[0], date_tup[1]), max(date_tup[0], date_tup[1])))
#    print(date_interval_list)
    date_interval_list = parsed_interval_list
    date_interval_list.sort(key = lambda x: x[0])

    last_interval = (datetime.datetime(1,1,1,0,0), datetime.datetime(1,1,1,0,0))
    experience = 0
    for i in range(n):
        if(date_interval_list[i][0] <= last_interval[1] and date_interval_list[i][1] > last_interval[1]):
            last_interval = (last_interval[0], date_interval_list[i][1])
        elif(date_interval_list[i][0] > last_interval[1]):
            experience += (last_interval[1] - last_interval[0]).days
            last_interval = date_interval_list[i]
        #print(experience)
        #print(last_interval)
    experience += (last_interval[1] - last_interval[0]).days
    return experience/365

def get_dateTime(date):
    if date.lower() in NOW.split('|'):
        return datetime.datetime.now()
    else:
        date = date.replace("’"," ").replace("'"," ")
        return dateparser.parse(date)

def get_year_couple(date_list):
    new_date_list = []
    for dates in date_list:
        new_dates = []
        for dt in dates:
            new_dates.extend(dt.split(COUPLE_DELI))
        new_date_list.append(new_dates)
    return new_date_list

def get_suitable_couple(date_list):
    new_date_list = []
    for dates in date_list:
        try:
            if len(dates)>1:
                dt1 = get_dateTime(dates[0])
                dt2 = get_dateTime(dates[1])
                new_date_list.append([dt1, dt2])
        except:
            #print("yo", dates[0], dates[1])
            continue
    return new_date_list

def get_pairs_diff_sum(date_list):
    date_list = get_year_couple(date_list)
    date_list = get_suitable_couple(date_list)
    #print("Getting pair diff sum",date_list)
    return get_exp(date_list)


def get_years_exp(years):
    s = 0
    for yr in years:
        if "year" in yr.lower() or "yrs" in yr.lower():
            e = get_num(yr.lower().replace("year", "").replace("yrs", ""))
        elif "month" in yr.lower():
            e = get_num(yr.lower().replace("month", ""))/12
        if e>=0:
            s+=e
    return s

def get_largest_years_exp(years):
    s = []
    for yr in years:
        if "year" in yr.lower() or "yrs" in yr.lower():
            e = get_num(yr.lower().replace("year", "").replace("yrs", ""))
        elif "month" in yr.lower():
            e = get_num(yr.lower().replace("month", ""))/12
        if e>=0:
            s.append(e)
    s.sort()
    if len(s)>0:
        return s[-1]
    else:
        return 0

def get_experience(date_list):
    years = []
    dates_only_list = []
    for dates in date_list:
        dt = []
        dt_y = []
        for date in dates:
            if "yrs" in date.lower() or "year" in date.lower() or "month" in date.lower():
                dt_y.append(date)
            else:
                dt.append(date)
        if dt_y!=[]:
            years.append(dt_y)
        if dt!=[]:
            dates_only_list.append(dt)
    #print(years, dates_only_list)
    years = [item for sublist in years for item in sublist]
    exp = get_largest_years_exp(years)
    if exp >=1:
        return exp
    exp = get_pairs_diff_sum(dates_only_list)
    return exp

def read_file(file_address, format='list'):
    with open(file_address, 'rb') as f:
        contents = f.read()
    if format=='list':
        return([t for t in contents.decode("utf-8", "ignore").split('\n') if t!=''])
    elif format=='text':
        return contents.decode("utf-8", "ignore")


def get_years_of_experience(file_address):
    try:
        fl = read_file(file_address)
    except:
        return -2
    return get_years_of_experience_text(fl)
    
def get_years_of_experience_text(fl):
    if ' '.join(fl).lower().find("experience") == -1:
        return -1
    try:
        text = [clean_text(t) for t in fl]
        x,y = get_experience_block(text)
        dates = get_dates_list(text[0:x]) + get_dates_list(text[y:])
        #print("Done good",file_address)
        return get_experience(dates)
    except Exception as e:
        #print(e)
        return -1
# test = ['AMIT KUMAR MISHRA', ' I intend to be a part of an organization where I can constantly learn and develop my technical skills and', 'make best use of it.', 'E-Mail ID: mishra1.amit29@gmail.com', 'Mobile/Phone: +91-8583885211                                                    Date of Birth: 29/11/1995', 'ACADEMICS', 'Qualification                 Institute                 Board/University             Year            %/CGPA', ' Graduation       RCC Institute of Information', '  (B. Tech,          Technology, Kolkata                   MAKAUT, WB                2019              7.17', '    ECE)', "     XII             St. Xavier's Institution,               ISC,CISCE               2015             85.25%", '                         Panihati, Kolkata', "      X              St. Xavier's Institution,              ICSE,CISCE               2012             83.40%", '                         Panihati, Kolkata', 'CERTIFICATES / PUBLICATIONS', 'Certifications/Publications           Certificate for Completion of C Training (Spoken Tutorial) offered by Spoken', '                                      Tutorial Project, IIT Bombay, funded by National Mission on Education', '                                      through ICT, MHRD, Govt., of India.', '                                      Certificate for Completion of JAVA Training (Spoken Tutorial) offered by', '                                      Spoken Tutorial Project, IIT Bombay, funded by National Mission on Education', '                                      through ICT, MHRD, Govt., of India.', '                                      Certificate for Completion of PYTHON Training (Spoken Tutorial) offered by', '                                      Spoken Tutorial Project, IIT Bombay, funded by National Mission on Education', '                                      through ICT, MHRD, Govt., of India.', '                                      Certificate for Completion of CCNA Networking Training.', 'WORK EXPERIENCE / INTERNSHIP PROJECT/TRAININGS', '  Organization/Company Name with', '                                                                Position & Job Responsibility', '                duration', '            Brillica Services                       CISCO Certified CCNA Switching & Routing Training V3.0 .', '4Weeks   (20th June `18 to 13th July `18)', '\x0cINTERNSHIP/ PROJECTS', 'SL.NO.               PROJECT DOMAIN                DURATION                     PROJECT TASK', '     1.     Enterprise Network Security under      June \xad          Designing various Topologies using Switching', '            CISCO Routing & Switching              July`2018       & Routing Protocol', '            (Networking)', "     2.     IOT Based Irrigation System using      August'2018     Designing various Techniques to measure", "            Arduino (Microcontroller &             \xad May'2019      various Irrigation Techniques using Arduino", '            Microprocessor, IOT)                                   Programming', 'KEY SKILLS', '1.               TECHNOLOGIES / TOOLS                       MS OFFICE, MATLAB', '2.               PROGRAMMING LANGUAGE                       BASICS OF C, JAVA & SQL', '3.               OPERATING SYSTEM                           WINDOWS 10, 8.1, 8, 7, XP & Ubuntu', 'ACHIEVEMENTS', '          Got Cash Award of Rs. 1000 & Monthly Scholarship of Rs. 200 in the academic year 2012 on behalf', '          of Suburban Educational Society for my academic proficiency in I.C.S.E 2012 Examination for', '          period of April,2012 to September,2012', '          Got Cash Award of Rs.2500 in the academic year 2015 on behalf of Suburban Educational Society', '          for my academic proficiency in I.S.C 2015', '          Got Cash Award in TechFest in College in 4th Semester, 2017', 'EXTRA CURRICULAR ACTIVITIES', 'Participation                      Participated in Robotics (Inter-college Fest)', '                                   Participated in Capgemini Tech Challenge.', '                                   Participated in Technical quiz organized by "The Institution of', '                                   Engineers".', "                                   Member of School's Eco-club and Social service Club.", '                                   Volunteer at "MAY-DAY" event at school.', 'Career Interest                    Keen interest in Learning new Technologies.', '                                   Interest in learning Mobile Application Development.', '                                   Interest in Front and Backend Development on Web', 'Hobbies                            Blog Writing(Mostly Travelling).', '                                   Volunteer Work', '                                   Music Playing & Listening', '                                   Sports: Badminton, Cricket, Bicycling', '                                   Teaching Children', '\x0cSOFT SKILLS', '             TIME MANAGEMENT ABILITY                           SELF MOTIVATED', '             DECISION MAKING                                   EXTROVERT & AGILE', '             ADAPTIBLITY                                       ETHICS & MATH SKILLS', 'LANGUAGES KNOWN', '        LANGUAGES                  Read                       Write                     Speak', '          English                  YES                        YES                       YES', '           Hindi                   YES                        YES                       YES', '          Bengali                  YES                        YES                       YES', '          Bhojpuri                 YES                        YES                       YES', '         I hereby declare that the above information is true and correct to the best of my knowledge.', '         20 /07 / 2019', 'Date:                                                Signature:', '\x0c']
# print (get_years_of_experience_text(test))






