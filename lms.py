import requests
from requests.api import head
from lxml import html, etree
import pandas as pd
import mysql.connector
import sqlalchemy
import pandas as pd
from pandas.io import sql
from sqlalchemy.types import String, Date, DateTime, Integer
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed



def login(url,headers, login, pwd, s):
    r = s.get(url, headers=headers)   
    tree = html.fromstring(r.text)
    token= tree.xpath("//*[@id='login_form']/input[2]")[0]
    payload = {
    'pseudonym_session[unique_id]': login,
    'pseudonym_session[password]': pwd,
    'pseudonym_session[remember_me]':1,
    'authenticity_token':token.value
    }
    s.post('https://canvas.instructure.com/login/canvas', data=payload)
    return s

def extract_grades(response,course,student):
    grades=[]
    grade={}
    grade_rows = html.fromstring(response.text).find_class('student_assignment editable')
    for grade_row in grade_rows:
        grade['student_id']=student
        grade['course_id']=course
        grade['name'] = grade_row.xpath('./th/a')[0].text
        grade['link'] = grade_row.xpath('./th/a')[0].values()[0]
        grade['assignment_id']=re.search("assignments/(.*)/submissions", grade['link']).group(1)
        grade['due']= grade_row.xpath('./td')[0].text.strip()
        grade['status']= grade_row.xpath('./td')[1].text.strip()
        grade['score']=grade_row.xpath('./td')[2].text.strip()
        grade['grade'] =grade_row.xpath('.//td[3]/div/span[3]/span')[0].values()[1]
        grade['out_of']=grade_row.xpath('.//td[4]')[0].text.strip()
        #print(grade)
        grades.append(grade.copy())
    #print(grades)
    return grades



def remove_multiple_keys(dictionary, keys):
    for key in keys:
        dictionary.pop(key, None)

    
def upload_table(table,data,mydb, database_connection):    
    #remove the old records from the db
    if len(data)>0:
        mycursor = mydb.cursor()
        mycursor.execute(f"delete FROM {table} where student_id={int(data[0]['student_id'])}")
        mydb.commit()
        mycursor.close()
        

    #upload new records
    try:
        frame           = pd.DataFrame(data).to_sql(table, database_connection, index= False, if_exists='append');
    except ValueError as vx:
        print(vx)
    except Exception as ex:   
        print(ex)
    else:
        print("Table %s updated successfully."%table);   
    finally:
        pass


def scrape_student(student_data, courses_keys_to_delete,assignment_keys_to_delete):
    LMS_URL=student_data['domain']#'https://canvas.instructure.com'
    API_URL = LMS_URL+'/api/v1/'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0',
    'accept-language':'en'}
    header_with_token = headers.copy()
    header_with_token['Authorization']= 'Bearer ' + student_data['bearer_token']

##get list of courses
    url = API_URL+"courses"
    courses = requests.get(url, headers=header_with_token).json()

    for course in courses:
        course['enrolled_as']=course['enrollments'][0]['type']
        course['student_id']=student_data['id']
        remove_multiple_keys(course, courses_keys_to_delete)

    #prepare requests session

    s=requests.session()

    #login using particular credentials
    s=login(LMS_URL+"/login/canvas" ,headers, student_data['login'], student_data['password'],s)

    assignments=[]
    submissions=[]
    grades=[]

    for course in courses:
        if course['enrolled_as']=='student':
                        
            #####scraping assignments

            url = API_URL+"courses/"+str(course['id'])+"/assignments"
            req=requests.get(url, headers=header_with_token)
            if req.status_code==200:
                for assignment in req.json():
                    #print(assignment)
                    remove_multiple_keys(assignment, assignment_keys_to_delete)
                    assignment['student_id']=student_data['id']
                    assignment['submission_types']=' '.join(assignment['submission_types'])
                    assignments.append(assignment)
            
            else:
                print(f"status code is {req.status_code}")
            
            #######scraping submissions

            url = API_URL+"courses/"+str(course['id'])+"/students/submissions"
            if req.status_code==200:
                submissions.append(requests.get(url, headers=header_with_token).json())
            
            ######scraping courses
            
            url=LMS_URL+'/courses/'+str(course['id'])+'/grades'
            req=s.get(url)
            if req.status_code==200:
                grades.append(extract_grades(req,str(course['id']),student_data['id']))
            else:
                print(f"status code is {req.status_code}")
        else:
            print(f"in {course['id']} student {student_data['login']} is enrolled as a {course['enrolled_as']}")
    return courses,assignments,grades,submissions


#those keys should be removed form the correspponding response
courses_keys_to_delete=['sis_course_id', 'integration_id', 'overridden_course_visibility','time_zone','account_id','uuid','grading_standard_id', 'is_public','default_view', 'root_account_id','enrollment_term_id','license','grade_passback_setting', 'public_syllabus', 'enrollments',
    'public_syllabus_to_auth','storage_quota_mb','is_public_to_auth_users','homeroom_course','course_color','apply_assignment_group_weights','calendar','blueprint', 'template', 'hide_final_grades',  'restrict_enrollments_to_course_dates']

assignment_keys_to_delete=['unlock_at', 'lock_at',   'assignment_group_id', 'grading_standard_id', 'created_at', 'updated_at', 'peer_reviews', 'automatic_peer_reviews',
     'position', 'grade_group_students_individually', 'anonymous_peer_reviews', 'group_category_id', 'post_to_sis', 'moderated_grading', 'omit_from_final_grade', 'intra_group_peer_reviews', 'anonymous_instructor_annotations', 'anonymous_grading', 
     'graders_anonymous_to_graders', 'grader_count', 'grader_comments_visible_to_graders', 'final_grader_id', 'grader_names_visible_to_final_grader', 'secure_params',   
     'max_name_length', 'in_closed_grading_period', 'is_quiz_assignment', 'can_duplicate', 'original_course_id', 'original_assignment_id', 'original_assignment_name', 'original_quiz_id', 'important_dates', 'muted',
     'anonymous_submissions', 'published', 'only_visible_to_overrides', 'locked_for_user', 'submissions_download_url', 'post_manually', 'anonymize_students', 'require_lockdown_browser']





database_username = 'root'
database_password = 'LMS-scraping.2021'
database_ip       = 'localhost'
database_name     = 'lms'
#create SQL alchemy engine
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?auth_plugin=mysql_native_password'.
                                               format(database_username, database_password, 
                                                  database_ip, database_name))
#create mysql connector
mydb = mysql.connector.connect(
  host=database_ip,
  user=database_username,
  password=database_password,
  database=database_name,
  auth_plugin='mysql_native_password')


students= pd.read_sql("select * from lms.students", database_connection)
start_time_global=time.time()
for k, student in students.iterrows():
    start_time_local=time.time()
    print(f"processing student {student['login']}")
    courses,assignments,grades,submissions=scrape_student(student,courses_keys_to_delete,assignment_keys_to_delete)
    upload_table('courses',courses,mydb,database_connection)
    upload_table('assignments',assignments,mydb,database_connection)
    
    if len(grades)>0:
        #print(grades[0])
        upload_table('grades',grades[0],mydb,database_connection)
    print(f"-------execution time: {round(time.time()-start_time_local,2)} seconds-------")
print(f"""-------total execution time: {round(time.time()-start_time_global,2)} seconds-------
\n-------average execution time: {round((time.time()-start_time_global)/students.shape[0], 2)} seconds-------""")
print("\n\n\nexisting submissions:")
print(submissions)
mydb.close()
