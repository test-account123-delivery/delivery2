from ftfcu_appworx import Apwx, JobTime
from email.message import EmailMessage
from pathlib import Path

import csv
import datetime
import smtplib

_version_ = 1.01


def run():
    apwx = get_apwx()
    parse_args(apwx)

    dbh = db_connect(apwx)

    path = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME

    if path.exists():
        raise FileExistsError(f'Output file already exists at {path}.')

    # determine if full scan for records vs. fixed date & get sql
    is_full_cleanup = True if apwx.args.FULL_CLEANUP_YN.upper() == 'Y' else None
    run_date = apwx.args.RUN_DATE

    # is_full_cleanup and run_date params are mutually exclusive - exit job if either run_date/is_full_cleanup
    # parameters both exist, or neither exist
    if is_full_cleanup is not None and run_date is not None:
        raise Exception(f'Parameter error - IS_FULL_CLEANUP and RUN_DATE params are mutually exclusive. '
                        f'Only one parameter value should be provided: '
                        f'IS_FULL_CLEANUP={is_full_cleanup} and RUN_DATE={run_date}.')

    if is_full_cleanup is None and run_date is None:
        raise Exception(f'Parameter error - no RUN_DATE parameter provided, and IS_FULL_CLEANUP not selected: '
                        f'IS_FULL_CLEANUP={is_full_cleanup} and RUN_DATE={run_date}.')

    # start job
    sql = get_sql(is_full_cleanup=is_full_cleanup, run_date=run_date)

    pers_records, org_records = fetch_records(dbh, sql)

    successes = list()
    fails = list()

    successes, fails = update_stdl_userfield(apwx, pers_records, dbh, table_name='persuserfield', col_name='persnbr')
    o_successes, o_fails = update_stdl_userfield(apwx, org_records, dbh, table_name='orguserfield', col_name='orgnbr')

    successes.extend(o_successes)
    fails.extend(o_fails)

    path = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME

    if successes:
        write_report(path, successes, write_mode='w')
    if fails:
        write_report(path, fails, write_mode='a+')

    # send email if fails and at least one recipient
    if fails and apwx.args.EMAIL_RECIPIENTS and apwx.args.SEND_EMAIL_YN == 'Y':
        smtp_server = apwx.args.SMTP_SERVER
        from_addr = apwx.args.FROM_EMAIL_ADDR
        recipients = apwx.args.EMAIL_RECIPIENTS.split(',')

        send_email(smtp_server, from_addr, recipients)
    elif fails and apwx.args.EMAIL_RECIPIENTS is None and apwx.args.SEND_EMAIL_YN == 'Y':
        print(f'SEND_EMAIL_YN == {apwx.args.SEND_EMAIL_YN}. No email recipients found.')
    else:
        print(f'No failed inserts/updates to report. No notification email(s) sent.')

    dbh.close()

    return True


def get_apwx():
    apwx = Apwx(['OSIUPDATE', 'OSIUPDATE_PW'])
    apwx.print_messages = None
    return apwx


def parse_args(apwx):
    parser = apwx.parser

    parser.add_arg('TNS_SERVICE_NAME', type=str, required=True)
    parser.add_arg('OUTPUT_FILE_PATH', type=parser.dir_validator, required=True)
    parser.add_arg('OUTPUT_FILE_NAME', type=r'.csv$', required=True)
    # validate RUN_DATE parameter value via datetime object, then convert to string
    parser.add_arg('RUN_DATE', type=lambda d: datetime.datetime.strptime(d, '%m-%d-%Y').strftime('%m-%d-%Y'),
                   required=False)
    parser.add_arg('RPTONLY_YN', choices=['Y', 'N'], required=True)
    parser.add_arg('FULL_CLEANUP_YN', choices=['Y', 'N'], required=True)
    parser.add_arg('SEND_EMAIL_YN', choices=['Y', 'N'], required=True)
    parser.add_arg('EMAIL_RECIPIENTS', type=r'([\w\.]+@firsttechfed\.com,?)+', ignore_case=True, required=True)
    parser.add_arg('SMTP_SERVER', type=str, required=True)
    parser.add_arg('FROM_EMAIL_ADDR', type=r'[\w\.]+@firsttechfed\.com', default='AM_PROD@firsttechfed.com',
                   required=False, ignore_case=True)

    apwx.parse_args()
    return True


def db_connect(apwx):
    dbh = apwx.db_connect()

    if apwx.args.RPTONLY_YN.upper() == 'N':
        dbh.autocommit = True
    else:
        dbh.autocommit = False

    return dbh


def get_sql(is_full_cleanup=None, run_date=None):
    close_date_join = ''

    if is_full_cleanup is None and run_date is not None:
        close_date_join = f"""
            JOIN acctacctstathist ah
                ON a.acctnbr = ah.acctnbr
                AND ah.acctstatcd = a.curracctstatcd 
                AND TRUNC(ah.effdatetime) = TO_DATE('{run_date}', 'mm-dd-yyyy')
                AND ah.timeuniqueextn = (
                    SELECT MAX(timeuniqueextn)
                    FROM acctacctstathist
                    WHERE acctnbr = ah.acctnbr
                    AND acctstatcd = ah.acctstatcd
                    AND effdatetime = ah.effdatetime
                )
        """
    else:
        close_date_join = f"""
            JOIN acctacctstathist ah
                ON a.acctnbr = ah.acctnbr
                AND ah.acctstatcd = a.curracctstatcd 
                AND ah.effdatetime = (
                    SELECT MAX(effdatetime)
                    FROM acctacctstathist
                    WHERE acctnbr = ah.acctnbr
                    AND acctstatcd = ah.acctstatcd
                    AND timeuniqueextn = ah.timeuniqueextn
                )
                AND ah.timeuniqueextn = (
                    SELECT MAX(timeuniqueextn)
                    FROM acctacctstathist
                    WHERE acctnbr = ah.acctnbr
                    AND acctstatcd = ah.acctstatcd
                    AND effdatetime = ah.effdatetime
                )           
        """

    # start query
    sql = f'''
        SELECT DISTINCT
            'pers' as entity_type,
            p.persnbr as entity_number,
            a.acctnbr,
            p.firstname || ' ' || p.lastname as entity_name,
            TO_CHAR(ah.effdatetime, 'mm-dd-yyyy') AS close_date,
            pu.value curr_stdl

        FROM pers p

        JOIN acct a
            ON p.persnbr = a.taxrptforpersnbr

        LEFT JOIN persuserfield pu
            ON p.persnbr = pu.persnbr
            AND pu.userfieldcd = 'STDL'
            AND pu.value != 'PAPR'     

        {close_date_join}

        WHERE a.mjaccttypcd IN ('SAV', 'CNS', 'MTG', 'CML')
        AND a.curracctstatcd = 'CLS'

        AND (

            NOT EXISTS

            (  -- is not TRO on another (i.e. not the membership DSA) active deposit account or loan
                 SELECT 1
                 FROM acct
                 WHERE taxrptforpersnbr = a.taxrptforpersnbr
                 AND mjaccttypcd IN ('SAV', 'CNS', 'MTG', 'EXT', 'CML', 'CK', 'TD')
                 AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                 AND rownum = 1
            )

            OR EXISTS

            (
                 SELECT 1  -- The Person or Organization’s only open ‘Account’ is a Safe Deposit Box.
                 FROM acct
                 WHERE taxrptforpersnbr = a.taxrptforpersnbr
                 AND mjaccttypcd = 'LEAS'
                 AND currmiaccttypcd = 'SDB'
                 AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                 AND rownum = 1
                 AND NOT EXISTS (
                     SELECT 1
                     FROM acct
                     WHERE taxrptforpersnbr = a.taxrptforpersnbr
                     AND mjaccttypcd != 'LEAS'
                     AND currmiaccttypcd != 'SDB'
                     AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                     AND rownum = 1
                 )
            )

         OR EXISTS

             (  -- The Person or Organization’s only open ‘Account’ is a RTMT plan
                 SELECT 1
                 FROM acct
                 WHERE taxrptforpersnbr = a.taxrptforpersnbr
                 AND mjaccttypcd = 'RTMT'
                 AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                 AND rownum = 1
                 AND NOT EXISTS (
                     SELECT 1
                     FROM acct
                     WHERE taxrptforpersnbr = a.taxrptforpersnbr
                     AND mjaccttypcd != 'RTMT'
                     AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                 )
            )
        )

        UNION

        SELECT DISTINCT
            'org' as entity_type,
            o.orgnbr as entity_number,
            a.acctnbr,
            o.orgname as entity_name,
            TO_CHAR(ah.effdatetime, 'mm-dd-yyyy') AS close_date,
            ou.value curr_stdl


        FROM org o

        JOIN acct a
            ON o.orgnbr = a.taxrptfororgnbr

        LEFT JOIN orguserfield ou
            ON o.orgnbr = ou.orgnbr
            AND ou.userfieldcd = 'STDL'
            AND ou.value != 'PAPR'  

        {close_date_join}

        WHERE a.mjaccttypcd IN ('SAV', 'CNS', 'MTG', 'CML')
        AND a.curracctstatcd = 'CLS'
        AND (
            NOT EXISTS
            (  -- is not TRO on another (i.e. not the membership DSA) active deposit account or loan
                SELECT 1
                FROM acct
                WHERE taxrptfororgnbr = a.taxrptfororgnbr
                AND mjaccttypcd IN ('SAV', 'CNS', 'MTG', 'EXT', 'CML', 'CK', 'TD')
                AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                AND rownum = 1
            )

            OR EXISTS

            (
                SELECT 1  -- The Person or Organization’s only open ‘Account’ is a Safe Deposit Box.
                FROM acct
                WHERE taxrptfororgnbr = a.taxrptfororgnbr
                AND mjaccttypcd = 'LEAS'
                AND currmiaccttypcd = 'SDB'
                AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                AND rownum = 1
                AND NOT EXISTS (
                    SELECT 1
                    FROM acct
                    WHERE taxrptfororgnbr = a.taxrptfororgnbr
                    AND mjaccttypcd != 'LEAS'
                    AND currmiaccttypcd != 'SDB'
                    AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                    AND rownum = 1
                )
            )

            OR EXISTS

            (  -- The Person or Organization’s only open ‘Account’ is a RTMT plan
                SELECT 1
                FROM acct
                WHERE taxrptfororgnbr = a.taxrptfororgnbr
                AND mjaccttypcd = 'RTMT'
                AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                AND rownum = 1
                AND NOT EXISTS (
                    SELECT 1
                    FROM acct
                    WHERE taxrptfororgnbr = a.taxrptfororgnbr
                    AND mjaccttypcd != 'RTMT'
                    AND curracctstatcd IN ('ACT', 'IACT', 'DORM', 'NPFM')
                    AND rownum = 1
                )
            )
        )

    '''

    return sql


def fetch_records(dbh, sql):
    with dbh.cursor() as cursor:
        cursor.execute(sql)

        # change result format from tuples to dictionary
        columns = [col[0] for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))

        records = cursor.fetchall()

        # split records by entity types & remove dups w/
        pers_records = [r for r in records if r['ENTITY_TYPE'] == 'pers']
        org_records = [r for r in records if r['ENTITY_TYPE'] == 'org']

    return pers_records, org_records


def update_stdl_userfield(apwx, records, dbh, table_name=None, col_name=None):
    filtered_nbrs = list(set(r['ENTITY_NUMBER'] for r in records))
    entity_nbrs = [[r] for r in filtered_nbrs]
    successes = []
    fails = []

    sql_merge = f''' 
                MERGE INTO {table_name} pu
                USING ( SELECT
                            :1 entity_nbr
                      FROM DUAL
                ) x 
                ON (pu.{col_name} = x.entity_nbr 
                AND pu.userfieldcd = 'STDL' )
                WHEN MATCHED THEN
                    UPDATE SET
                        pu.value = 'PAPR',
                        pu.datelastmaint = SYSDATE
                WHEN NOT MATCHED THEN
                    INSERT (
                        {col_name},
                        userfieldcd,
                        value,
                        datelastmaint
                    )
                    VALUES (
                        x.entity_nbr,
                        'STDL',
                        'PAPR',
                        SYSDATE
                    )   
                '''

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


def send_email(smtp_server, from_addr, recipients):
    """ """
    msg = EmailMessage()

    msg['Subject'] = f'Statement Delivery Method Update Alert'
    msg['From'] = from_addr
    msg['To'] = recipients

    content = f'One or more statement delivery method updates has failed.  Please see log file(s) in Identifi.'
    msg.set_content(content)

    s = smtplib.SMTP(smtp_server)
    s.send_message(msg)
    s.quit()
    return True


if _name_ == '_main_':
    JobTime.print_start()
    run()
    JobTime.print_end()
