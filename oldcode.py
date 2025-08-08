from ftfcu_appworx import Apwx, JobTime
from email.message import EmailMessage
from pathlib import Path
from enum import Enum, auto
from typing import Optional, Any
from dataclasses import dataclass
from jinja2 import Environment, FileSystemLoader
from oracledb import Connection as DbConnection
import os
import yaml

import csv
import datetime
import smtplib

_version_ = 1.01


class AppWorxEnum(Enum):
    """Define AppWorx arguments here to avoid hard-coded strings"""
    
    TNS_SERVICE_NAME = auto()
    CONFIG_FILE_PATH = auto()
    OUTPUT_FILE_PATH = auto()
    OUTPUT_FILE_NAME = auto()
    RUN_DATE = auto()
    RPTONLY_YN = auto()
    FULL_CLEANUP_YN = auto()
    SEND_EMAIL_YN = auto()
    EMAIL_RECIPIENTS = auto()
    SMTP_SERVER = auto()
    SMTP_PORT = auto()
    SMTP_USER = auto()
    SMTP_PASSWORD = auto()
    FROM_EMAIL_ADDR = auto()
    TEST_EMAIL_ADDR = auto()

    def __str__(self):
        return self.name


@dataclass
class ScriptData:
    """Class that holds all the structures and data needed by the script"""

    apwx: Apwx
    dbh: DbConnection
    config: Any
    email_template: Any


def run(apwx: Apwx):
    """The main logic of the script goes here"""
    script_data = initialize(apwx)
    pers_records, org_records = fetch_records(script_data)
    successes, fails = process_records(script_data, pers_records, org_records)
    write_report_file(script_data, successes, fails)
    send_notification_email(script_data, fails)
    script_data.dbh.close()

    return True


def initialize(apwx: Apwx) -> ScriptData:
    """Initialize objects required by the script to call external systems"""
    config = get_config(apwx)
    return ScriptData(
        apwx=apwx,
        dbh=dna_db_connect(apwx),
        config=config,
        email_template=get_email_template(config),
    )


def get_apwx() -> Apwx:
    """Creates a new authenticated context for Appworx"""
    return Apwx(['OSIUPDATE', 'OSIUPDATE_PW'])


def parse_args(apwx: Apwx) -> Apwx:
    """Parses the arguments to the script"""
    parser = apwx.parser

    parser.add_arg(str(AppWorxEnum.TNS_SERVICE_NAME), type=str, required=True)
    parser.add_arg(str(AppWorxEnum.CONFIG_FILE_PATH), type=r"(.yml|.yaml)$", required=True)
    parser.add_arg(str(AppWorxEnum.OUTPUT_FILE_PATH), type=parser.dir_validator, required=True)
    parser.add_arg(str(AppWorxEnum.OUTPUT_FILE_NAME), type=r'.csv$', required=True)
    # validate RUN_DATE parameter value via datetime object, then convert to string
    parser.add_arg(str(AppWorxEnum.RUN_DATE), type=lambda d: datetime.datetime.strptime(d, '%m-%d-%Y').strftime('%m-%d-%Y'),
                   required=False)
    parser.add_arg(str(AppWorxEnum.RPTONLY_YN), choices=['Y', 'N'], required=True)
    parser.add_arg(str(AppWorxEnum.FULL_CLEANUP_YN), choices=['Y', 'N'], required=True)
    parser.add_arg(str(AppWorxEnum.SEND_EMAIL_YN), choices=['Y', 'N'], required=True)
    parser.add_arg(str(AppWorxEnum.EMAIL_RECIPIENTS), type=r'([\w\.]+@firsttechfed\.com,?)+', ignore_case=True, required=True)
    parser.add_arg(str(AppWorxEnum.SMTP_SERVER), type=str, required=True)
    parser.add_arg(str(AppWorxEnum.SMTP_PORT), type=int, required=True)
    parser.add_arg(str(AppWorxEnum.SMTP_USER), type=str, required=True)
    parser.add_arg(str(AppWorxEnum.SMTP_PASSWORD), type=str, required=True)
    parser.add_arg(str(AppWorxEnum.FROM_EMAIL_ADDR), type=r'[\w\.]+@firsttechfed\.com', default='AM_PROD@firsttechfed.com',
                   required=False, ignore_case=True)
    parser.add_arg(str(AppWorxEnum.TEST_EMAIL_ADDR), type=str, required=False)

    apwx.parse_args()
    return apwx


def dna_db_connect(apwx: Apwx) -> DbConnection:
    """Creates a connection to DNA database"""
    dbh = apwx.db_connect(autocommit=False)
    
    if apwx.args.RPTONLY_YN.upper() == 'N':
        dbh.autocommit = True
    else:
        dbh.autocommit = False

    return dbh


def fetch_records(script_data: ScriptData) -> tuple[list[dict], list[dict]]:
    """Fetch records from database using config-driven SQL"""
    print("Fetching records for processing")
    
    # Check for parameter validation
    apwx = script_data.apwx
    is_full_cleanup = True if apwx.args.FULL_CLEANUP_YN.upper() == 'Y' else None
    run_date = apwx.args.RUN_DATE

    # Validate mutually exclusive parameters
    if is_full_cleanup is not None and run_date is not None:
        raise Exception(f'Parameter error - IS_FULL_CLEANUP and RUN_DATE params are mutually exclusive. '
                        f'Only one parameter value should be provided: '
                        f'IS_FULL_CLEANUP={is_full_cleanup} and RUN_DATE={run_date}.')

    if is_full_cleanup is None and run_date is None:
        raise Exception(f'Parameter error - no RUN_DATE parameter provided, and IS_FULL_CLEANUP not selected: '
                        f'IS_FULL_CLEANUP={is_full_cleanup} and RUN_DATE={run_date}.')

    # Get SQL from config and build the join clause
    sql_template = script_data.config["sql_queries"]["get_records"]
    
    if is_full_cleanup is None and run_date is not None:
        close_date_join = script_data.config["join_fragments"]["date_specific"].replace("{{run_date}}", run_date)
    else:
        close_date_join = script_data.config["join_fragments"]["full_cleanup"]
    
    # Replace the join placeholder in the main query
    sql = sql_template.replace("{{close_date_join}}", close_date_join)
    
    records = execute_sql_select(script_data.dbh, sql)

    # Split records by entity types
    pers_records = [r for r in records if r['ENTITY_TYPE'] == 'pers']
    org_records = [r for r in records if r['ENTITY_TYPE'] == 'org']
    
    print(f"Found {len(pers_records)} person records and {len(org_records)} organization records")
    return pers_records, org_records


def process_records(script_data: ScriptData, pers_records: list[dict], org_records: list[dict]) -> tuple[list, list]:
    """Process records and update the database"""
    print("Processing records for database updates")
    
    # Check if output file already exists
    apwx = script_data.apwx
    path = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME
    if path.exists():
        raise FileExistsError(f'Output file already exists at {path}.')
    
    successes = list()
    fails = list()

    # Update person records
    p_successes, p_fails = update_stdl_userfield(script_data, pers_records, table_name='persuserfield', col_name='persnbr')
    # Update organization records  
    o_successes, o_fails = update_stdl_userfield(script_data, org_records, table_name='orguserfield', col_name='orgnbr')

    successes.extend(p_successes)
    successes.extend(o_successes)
    fails.extend(p_fails)
    fails.extend(o_fails)
    
    print(f"Processing complete: {len(successes)} successes, {len(fails)} failures")
    return successes, fails


def update_stdl_userfield(script_data: ScriptData, records: list[dict], table_name: str, col_name: str) -> tuple[list, list]:
    """Update STDL userfield for given records using config-driven SQL"""
    if not records:
        return [], []
        
    filtered_nbrs = list(set(r['ENTITY_NUMBER'] for r in records))
    entity_nbrs = [[r] for r in filtered_nbrs]
    successes = []
    fails = []
    
    # Get SQL from config based on table type
    if table_name == 'persuserfield':
        sql_merge = script_data.config["sql_queries"]["update_pers_stdl"]
    else:
        sql_merge = script_data.config["sql_queries"]["update_org_stdl"]

    apwx = script_data.apwx
    dbh = script_data.dbh
    sth = dbh.cursor()

    sth.executemany(sql_merge, entity_nbrs, batcherrors=True)

    batch_errors = sth.getbatcherrors()

    if batch_errors:
        for error in batch_errors:
            # get index
            error_idx = error.offset

            # get entity nbr from merge list
            merge_ent_nbr = entity_nbrs[error_idx]

            print(f'Error {error.message} at row {error_idx} during merge.'
                  f"{col_name}: {merge_ent_nbr}")

            # if failed entity nbr exists, add fail message to record for reporting
            for rec in records:
                if rec['ENTITY_NUMBER'] == merge_ent_nbr:
                    fails.append(
                        (
                            merge_ent_nbr,
                            rec['ACCTNBR'],
                            rec['ENTITY_TYPE'],
                            rec['CLOSE_DATE'],
                            'Fail',
                         )
                    )

    if apwx.args.RPTONLY_YN.upper() == 'N':
        dbh.commit()
    else:
        dbh.rollback()

    successes = [(r['ENTITY_NUMBER'], r['ACCTNBR'], r['ENTITY_TYPE'], r['CLOSE_DATE'], 'Success') for r in records
                 if r['ENTITY_NUMBER'] not in fails]

    print(f'Number Of Updated Records in {table_name} table : ', sth.rowcount, '\n')

    sth.close()

    return successes, fails


def write_report_file(script_data: ScriptData, successes: list, fails: list):
    """Generate the output report file"""
    print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Writing report file")
    apwx = script_data.apwx
    path = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME
    
    if successes:
        write_report(path, successes, write_mode='w')
    if fails:
        write_report(path, fails, write_mode='a+')


def send_notification_email(script_data: ScriptData, fails: list):
    """Send email notification if there are failures"""
    apwx = script_data.apwx
    
    if fails and apwx.args.EMAIL_RECIPIENTS:
        recipients = apwx.args.EMAIL_RECIPIENTS.split(',')
        successful, message = send_email(script_data, recipients)
        print(f"Email notification result: {message}")
    elif fails and apwx.args.EMAIL_RECIPIENTS is None and send_email_enabled(apwx):
        print(f'SEND_EMAIL_YN == {apwx.args.SEND_EMAIL_YN}. No email recipients found.')
    else:
        print(f'No failed inserts/updates to report. No notification email(s) sent.')


def write_report(path, records, write_mode):
    run_date = datetime.datetime.today().strftime('%m-%d-%Y')

    with open(path, write_mode, newline='') as csv_file:
        writer = csv.writer(csv_file)

        if write_mode == 'w':
            header = ['ENTITY_NBR', 'ACCTNBR', 'ENTITY_TYPE', 'CLOSE_DATE', 'RESULT']
            writer.writerow(header)

        for rec in records:
            # create dict from header (keys) and tuple (values) - used to ensure field order is consistent
            r = dict(zip(header, rec))

            row = [
                r['ENTITY_NBR'],
                r['ACCTNBR'],
                r['ENTITY_TYPE'],
                r['CLOSE_DATE'],
                r['RESULT']
            ]

            writer.writerow(row)

    return True


def send_email(script_data: ScriptData, recipients: list) -> (bool, str):
    """Send email notification for failed updates"""
    apwx = script_data.apwx
    to_address = recipients[0] if recipients else None
    if apwx.args.TEST_EMAIL_ADDR:
        to_address = apwx.args.TEST_EMAIL_ADDR
    from_address = apwx.args.FROM_EMAIL_ADDR

    if not to_address:
        return False, "No email recipients"

    # Create the email content
    email_content = generate_email_content(script_data)
    email_message = generate_email_message(from_address, to_address, email_content)

    # Don't send if we're on local dev env or the SEND_EMAIL_YN parameter is N
    if is_local_environment() or not send_email_enabled(apwx):
        return False, "Email Send Disabled"

    try:
        send_smtp_request(apwx, from_address, to_address, email_message)
        return True, "Email Sent"
    except Exception as e:
        print(f"An exception was encountered sending email to {to_address}.", e)
        return False, "Email Failed"


def generate_email_message(from_address: str, to_address: str, email_content: str) -> EmailMessage:
    """Generate email message object"""
    message = EmailMessage()
    message["Subject"] = "Statement Delivery Method Update Alert"
    message["From"] = f"First Tech Federal Credit Union <{from_address}>"
    message["To"] = to_address
    message.set_content(email_content)
    message.set_type("text/html")
    return message


def generate_email_content(script_data: ScriptData) -> str:
    """Generate custom email message content using template"""
    data = {
        "run_date": datetime.datetime.today().strftime('%m-%d-%Y'),
        "current_time": datetime.datetime.now().strftime('%H:%M:%S'),
    }
    return script_data.email_template.render(**data)


def send_smtp_request(apwx: Apwx, from_address: str, to_address: str, email_message: EmailMessage):
    """Send email request to SMTP server"""
    smtp_server = apwx.args.SMTP_SERVER
    smtp_port = int(apwx.args.SMTP_PORT)
    smtp_user = apwx.args.SMTP_USER
    smtp_password = apwx.args.SMTP_PASSWORD

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        print(f"Connecting to SMTP server {smtp_server}:{smtp_port}")
        server.connect(smtp_server, smtp_port)
        server.ehlo()
        server.starttls()
        server.ehlo()
        print(f"Logging into {smtp_server} as {smtp_user}")
        server.login(smtp_user, smtp_password)
        print(f"Sending email...")
        server.sendmail(from_address, to_address, email_message.as_string())


def is_local_environment() -> bool:
    """The absence of AW_HOME means AppWorx is not installed and it's local dev env"""
    return not bool(os.environ.get("AW_HOME"))


def send_email_enabled(apwx: Apwx) -> bool:
    """Check if email sending is enabled"""
    return apwx.args.SEND_EMAIL_YN.upper() == "Y"


def get_config(apwx: Apwx) -> Any:
    """Loads the config YAML file"""
    with open(apwx.args.CONFIG_FILE_PATH, "r") as f:
        return yaml.safe_load(f)


def get_email_template(config: Any) -> Any:
    """Returns the email template object used to generate HTML emails"""
    # Templates are in a 'templates' subfolder relative to the script
    template_directory: str = config["template_directory"]
    template_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), template_directory
    )
    file_loader = FileSystemLoader(template_dir)
    env = Environment(loader=file_loader)
    return env.get_template(config["template_file"])


def execute_sql_select(
    conn: DbConnection,
    sql_statement: str,
    sql_params: Optional[dict] = None,
) -> list[dict]:
    """Executes provided SELECT SQL statement
    Args:
        conn: Database connection object used to connect to DNA.
        sql_statement: The SQL statement to be executed.
        sql_params: Bind variables for the query
    Returns:
        SELECT statements will always return a list of dictionaries.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_statement, sql_params)
            column_names = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(column_names, args))
            return cursor.fetchall()
    except Exception as e:
        raise Exception(f"SQL error = {e}")


if __name__ == '__main__':
    JobTime().print_start()
    run(parse_args(get_apwx()))
    JobTime().print_end()