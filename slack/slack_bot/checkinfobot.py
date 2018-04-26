import os
import time
import datetime
import re
import pymysql
from prettytable import PrettyTable
from slackclient import SlackClient

connector = pymysql.connect(user='vcheve2', password = 'Setmaxhbod13!', host = 'localhost', database = 'CHECK_CPAR2', port= 3309)
cursor = connector.cursor()

# instantiate Slack client
slack_client = SlackClient("xoxb-341396859840-K1m7uNoOhmeUFLSz6OE9me2w")
# starterbot's user ID in Slack: value is assigned after the bot starts up
starterbot_id = "checkinfo_bot"

# constants
RTM_READ_DELAY = 1 # 1 second delay between reading from RTM
EXAMPLE_COMMAND = ["release","risk","diagnosis","age","help","age_diagnosis","risk_diagnosis","risk_age","engaged_diagnosis","enrolled_diagnosis","engaged_risk","enrolled_risk","engaged_age","enrolled_age","engaged_asthma","engaged_scd","engaged_diabetes","engaged_neurological","engaged_prematurity","engaged_other","engaged_na","enrolled_asthma","enrolled_scd","enrolled_diabetes","enrolled_neurological","enrolled_prematurity","enrolled_other","enrolled_na","engaged_low","engaged_high","engaged_medium","enrolled_low","enrolled_high","enrolled_medium","engaged_adolescents","engaged_adults","engaged_children","engaged_infant","engaged_youngadults","enrolled_adolescents","enrolled_adults","enrolled_children","enrolled_infant","enrolled_youngadults"]
MENTION_REGEX = "^<@(|[WU].+?)>(.*)"

def parse_bot_commands(slack_events):
    """
        Parses a list of events coming from the Slack RTM API to find bot commands.
        If a bot command is found, this function returns a tuple of command and channel.
        If its not found, then this function returns None, None.
    """
    for event in slack_events:
        if event["type"] == "message" and not "subtype" in event:
            user_id, message = parse_direct_mention(event["text"])
            if user_id == starterbot_id:
                return message, event["channel"]
    return None, None

def parse_direct_mention(message_text):
    """
        Finds a direct mention (a mention that is at the beginning) in message text
        and returns the user ID which was mentioned. If there is no direct mention, returns None
    """
    matches = re.search(MENTION_REGEX, message_text)
    # the first group contains the username, the second group contains the remaining message
    return (matches.group(1), matches.group(2).strip()) if matches else (None, None)

def type_diagnosis(type, diagnosis):

    if type == 'engaged':
        cursor.execute("select count(RecipientID) as count from pat_info_complete where E2 = 1 and Diagnosis_Category = '{}';".format(diagnosis))
    elif type == 'enrolled':
        cursor.execute("select count(RecipientID) as count from pat_info_complete where E4 = 1 and Diagnosis_Category = '{}';".format(diagnosis))
    
    diag_count = cursor.fetchone()
    diag_count = diag_count[0]
    print(diag_count)
    
    return diag_count

def type_risk(type, risk):

    if type == 'engaged':
        cursor.execute("select count(RecipientID) as count from pat_info_complete where E2 = 1 and Current_Risk = '{}';".format(risk))
    elif type == 'enrolled':
        cursor.execute("select count(RecipientID) as count from pat_info_complete where E4 = 1 and Current_Risk = '{}';".format(risk))
    
    risk_count = cursor.fetchone()
    risk_count = risk_count[0]
    print(risk_count)
    
    return risk_count

def type_age(type, age):

    if type == 'engaged':
        cursor.execute("select count(RecipientID) as count from pat_info_complete where E2 = 1 and Age_Category = '{}';".format(age))
    elif type == 'enrolled':
        cursor.execute("select count(RecipientID) as count from pat_info_complete where E4 = 1 and Age_Category = '{}';".format(age))
    
    age_count = cursor.fetchone()
    age_count = age_count[0]
    print(age_count)
    
    return age_count


def handle_command(command, channel):
    """
        Executes bot command if the command is known
    """
    # Default response is help text for the user
    default_response = "Not sure what you mean. Try *{}*.".format(EXAMPLE_COMMAND)

    # Finds and executes the given command, filling in response
    response = None
    # This is where you start to implement more commands!
    if command in EXAMPLE_COMMAND:
        if command == "release":
            cursor.execute("select max(releaseNum) from tum_hfs_stage_count_info;")
            releasenum = str(cursor.fetchone())
            cursor.execute("select max(endReleaseDate) from tum_hfs_stage_count_info;")  
            releasedate = cursor.fetchone()
            releasedate = releasedate[0]
            releasedate = releasedate.strftime('%Y-%m-%d')
            response = "Latest Release Number is {} and latest Release date is {}".format(releasenum.strip("(),"), releasedate)
        elif command == "risk":
            cursor.execute("select Current_Risk, count(RecipientID) as Count from pat_info_risk group by Current_Risk;")
            # mytable = from_cursor(cursor)
            risk = cursor.fetchall()
            risk = [list(x) for x in risk]
            t = PrettyTable(['Risk_Categories','Count'], border = True, padding_width = 5)

            for a in range(len(risk)):
                t.add_row(risk[a])
                       
            response = "The count of current risk categories are \n {}".format(t)
        elif command == "diagnosis":
            cursor.execute("select Diagnosis_Category, count(RecipientID) as Count from pat_info_dx_primary group by Diagnosis_Category;")
            diag = cursor.fetchall()
            diag = [list(x) for x in diag]
            t = PrettyTable(['Diagnosis_Categories','Count'],  border = True, padding_width = 5)

            for a in range(len(diag)):
                t.add_row(diag[a])
                       
            response = "The count of current diagnosis categories are \n {}".format(t)
        elif command == "age":
            cursor.execute("select Age_Category, count(Age_Category) as Count from pat_info_complete group by Age_Category;")
            age = cursor.fetchall()
            age = [list(x) for x in age]
            t = PrettyTable(['Age_Categories','Count'],  border = True, padding_width = 5)

            for a in range(len(age)):
                t.add_row(age[a])
                       
            response = "The count of current age categories are \n {}".format(t)
        elif command == "age_diagnosis":
            cursor.execute("select Age_Category, Diagnosis_Category, count(RecipientID) as Count from pat_info_complete group by age,Diagnosis_Category;")
            age = cursor.fetchall()
            age = [list(x) for x in age]
            t = PrettyTable(['Age_Category','Diagnosis_Category','Count'],  border = True, padding_width = 5)

            for a in range(len(age)):
                t.add_row(age[a])
                       
            response = "The count of current age categories are \n {}".format(t)

        elif command == "risk_diagnosis":
            cursor.execute("select Current_Risk, Diagnosis_Category, count(RecipientID) as Count from pat_info_complete group by Current_Risk,Diagnosis_Category;")
            risk = cursor.fetchall()
            risk = [list(x) for x in risk]
            t = PrettyTable(['Risk_Category','Diagnosis_Category','Count'],  border = True, padding_width = 5)

            for a in range(len(risk)):
                t.add_row(risk[a])
                       
            response = "The count of current age categories are \n {}".format(t)

        elif command == "risk_age":
            cursor.execute("select Current_Risk, Age_Category, count(RecipientID) as Count from pat_info_complete group by Current_Risk,Age_Category;")
            risk = cursor.fetchall()
            risk = [list(x) for x in risk]
            t = PrettyTable(['Risk_Category','Age_Category','Count'],  border = True, padding_width = 5)

            for a in range(len(risk)):
                t.add_row(risk[a])
                       
            response = "The count of current age categories are \n {}".format(t)

        elif command == "engaged_diagnosis":
            cursor.execute("select 'Engaged', Diagnosis_Category, count(RecipientID) as count from pat_info_complete where E2 = 1 group by Diagnosis_Category;")
            e_d = cursor.fetchall()
            e_d = [list(x) for x in e_d]
            t = PrettyTable(['Category','Diagnosis_Category','Count'],  border = True, padding_width = 5)

            for a in range(len(e_d)):
                t.add_row(e_d[a])
                       
            response = "The count of current age categories are \n {}".format(t)

        elif command == "enrolled_diagnosis":
            cursor.execute("select 'Enrolled', Diagnosis_Category, count(RecipientID) as count from pat_info_complete where E4 = 1 group by Diagnosis_Category;")
            er_d = cursor.fetchall()
            er_d = [list(x) for x in er_d]
            t = PrettyTable(['Category','Diagnosis_Category','Count'],  border = True, padding_width = 5)

            for a in range(len(er_d)):
                t.add_row(er_d[a])
                       
            response = "The count of current age categories are \n {}".format(t)

        elif command == "engaged_risk":
            cursor.execute("select 'Engaged', Current_Risk, count(RecipientID) as count from pat_info_complete where E2 = 1 group by Current_Risk;")
            e_r = cursor.fetchall()
            e_r = [list(x) for x in e_r]
            t = PrettyTable(['Category','Risk_Category','Count'],  border = True, padding_width = 5)

            for a in range(len(e_r)):
                t.add_row(e_r[a])
                       
            response = "The count of current age categories are \n {}".format(t)

        elif command == "enrolled_risk":
            cursor.execute("select 'Enrolled', Current_Risk, count(RecipientID) as count from pat_info_complete where E4 = 1 group by Current_Risk;")
            er_r = cursor.fetchall()
            er_r = [list(x) for x in er_r]
            t = PrettyTable(['Category','Risk_Category','Count'],  border = True, padding_width = 5)

            for a in range(len(er_r)):
                t.add_row(er_r[a])
                       
            response = "The count of current age categories are \n {}".format(t)

        elif command == "engaged_age":
            cursor.execute("select 'Engaged', Age_Category, count(RecipientID) as count from pat_info_complete where E2 = 1 group by Age_Category;")
            e_a = cursor.fetchall()
            e_a = [list(x) for x in e_a]
            t = PrettyTable(['Category','Risk_Category','Count'],  border = True, padding_width = 5)

            for a in range(len(e_a)):
                t.add_row(e_a[a])
                       
            response = "The count of current age categories are \n {}".format(t)

        elif command == "enrolled_age":
            cursor.execute("select 'Enrolled', Age_Category, count(RecipientID) as count from pat_info_complete where E4 = 1 group by Age_Category;")
            er_a = cursor.fetchall()
            er_a = [list(x) for x in er_a]
            t = PrettyTable(['Category','Diagnosis_Category','Count'],  border = True, padding_width = 5)

            for a in range(len(er_a)):
                t.add_row(er_a[a])
                       
            response = "The count of current age categories are \n {}".format(t)

        elif command == "engaged_asthma":
            en_as = type_diagnosis('engaged','Asthma')

            response = en_as
        elif command == "engaged_scd":
            en_as = type_diagnosis('engaged','SCD')

            response = en_as

        elif command == "engaged_diabetes":
            en_as = type_diagnosis('engaged','Diabetes')

            response = en_as

        elif command == "engaged_prematurity":
            en_as = type_diagnosis('engaged','Prematurity')

            response = en_as
        elif command == "engaged_neurological":
            en_as = type_diagnosis('engaged','Neurological')

            response = en_as

        elif command == "engaged_other":
            en_as = type_diagnosis('engaged','Other')

            response = en_as

        elif command == "engaged_na":
            en_as = type_diagnosis('engaged','NA')

            response = en_as

        elif command == "enrolled_asthma":
            en_as = type_diagnosis('enrolled','Asthma')

            response = en_as
        elif command == "enrolled_scd":
            en_as = type_diagnosis('enrolled','SCD')

            response = en_as

        elif command == "enrolled_diabetes":
            en_as = type_diagnosis('enrolled','Diabetes')

            response = en_as

        elif command == "enrolled_prematurity":
            en_as = type_diagnosis('enrolled','Prematurity')

            response = en_as
        elif command == "enrolled_neurological":
            en_as = type_diagnosis('enrolled','Neurological')

            response = en_as

        elif command == "enrolled_other":
            en_as = type_diagnosis('enrolled','Other')

            response = en_as

        elif command == "enrolled_na":
            en_as = type_diagnosis('enrolled','NA')

            response = en_as

        elif command == "enrolled_low":
            en_as = type_risk('enrolled','LOW')

            response = en_as

        elif command == "enrolled_high":
            en_as = type_risk('enrolled','HIGH')

            response = en_as

        elif command == "enrolled_medium":
            en_as = type_risk('enrolled','MEDIUM')

            response = en_as

        elif command == "engaged_low":
            en_as = type_risk('engaged','LOW')

            response = en_as

        elif command == "engaged_high":
            en_as = type_risk('engaged','HIGH')

            response = en_as

        elif command == "engaged_medium":
            en_as = type_risk('engaged','MEDIUM')
            
            response = en_as

        elif command == "engaged_adolescents":
            en_as = type_age('engaged','Adolescents')

            response = en_as

        elif command == "engaged_adults":
            en_as = type_age('engaged','Adults')

            response = en_as

        elif command == "engaged_children":
            en_as = type_age('engaged','children')
            
            response = en_as

        elif command == "engaged_infant":
            en_as = type_age('engaged','Infant')

            response = en_as

        elif command == "engaged_youngadults":
            en_as = type_age('engaged','YoungAdults')
            
            response = en_as
        elif command == "enrolled_adolescents":
            en_as = type_age('enrolled','Adolescents')

            response = en_as

        elif command == "enrolled_adults":
            en_as = type_age('enrolled','Adults')

            response = en_as

        elif command == "enrolled_children":
            en_as = type_age('enrolled','children')
            
            response = en_as

        elif command == "enrolled_infant":
            en_as = type_age('enrolled','Infant')

            response = en_as

        elif command == "enrolled_youngadults":
            en_as = type_age('enrolled','YoungAdults')
            
            response = en_as

        elif command == "help":
            response = '''Type *release* to get latest release number and release date \n Type *risk* to get the number of recipients in various risk categorizations \n Type *diagnosis* to get the number of recipeients with primary diagnoses \n Type *age* to know the number of recipients under various age categorizations \n Type *age_diagnosis* to get the number of recipients with that age and diagnoses categories \n Type *risk_diagnosis* to get the number of recipients with that risk and diagnoses categories \n Type *risk_age* to get the number of recipients with that risk and age categories \n Type *engaged_diagnosis* or *enrolled_diagnosis* to get the number of engaged/enrolled patients with correspoding diagnosis. The same is the case with *engaged_risk*, *enrolled_risk*, *engaged_age*, *enrolled_age*'''


    # Sends the response back to the channel
    slack_client.api_call(
        "chat.postMessage",
        channel=channel,
        text=response or default_response
    )

if __name__ == "__main__":
    if slack_client.rtm_connect(with_team_state=False):
        print("Checkinfo Bot connected and running!")
        # Read bot's user ID by calling Web API method `auth.test`
        starterbot_id = slack_client.api_call("auth.test")["user_id"]
        while True:
            command, channel = parse_bot_commands(slack_client.rtm_read())
            if command:
                handle_command(command, channel)
            time.sleep(RTM_READ_DELAY)
    else:
        print("Connection failed. Exception traceback printed above.")