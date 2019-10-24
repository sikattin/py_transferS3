#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
from mylogger.factory import StdoutLoggerFactory, \
                             FileLoggerFactory, \
                             RotationLoggerFactory

import os
import time
import boto3
import smtplib
import argparse
from botocore.exceptions import BotoCoreError
from socket import gethostname
from email.mime.text import MIMEText

LOGPATH_CLIENT = '/var/log/create_dailybackup_summary.log'
LOGPATH_MODULE = '/var/log/pymodule.log'
LOG_ROLLOVERSIZE = 100*1024*1024
SMTP_SERVER = '59.128.93.227'
TO_ADDR = ['notifi.tech.arad@nexon.co.jp']
CC_ADDR = ['notifi.tech.arad@nexon.co.jp']


class S3Client(object):
    """Amazon S3 client for operation.
    
    Args:
        object ([type]): [description]
    """

    def __init__(self, bucket: str, logger):
        self._bucket = bucket
        self._logger = logger
        self._s3 = boto3.client('s3')
    
    def list_objs(self, filterByKey: str):
        """list objects on the specified bucket.

        Args:
            filterByKey (str): Objects filtered by the specified key.

        Returns:
            dict

        Raises:
            botocore.exceptions.BotoCoreError
        """
        paginator = self._s3.get_paginator('list_objects_v2')
        resp_iter = paginator.paginate(
            Bucket=self._bucket,
            Prefix=filterByKey
        )
        res = resp_iter.build_full_result()
        del paginator
        del resp_iter
        return res

if __name__ == '__main__':

    def send_mail(subject: str, body: str, to_addr: list, cc_addr: list, from_addr=None):
        if from_addr is None:
            from_addr = '{0}@local'.format(gethostname())
        body = body
        
        mail = MIMEText(body)
        mail['To'] = ",".join(TO_ADDR)
        mail['Cc'] = ",".join(CC_ADDR)
        mail['From'] = from_addr
        mail['Subject'] = subject

        server = smtplib.SMTP(SMTP_SERVER)
        try:
            server.ehlo()
            server.sendmail(from_addr, [TO_ADDR, CC_ADDR], mail.as_string())
        finally:
            server.quit()

    description = 'Get informations about the specified objects from amazon s3 bucket'
    argparser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=description
    )
    argparser.add_argument('-b', '--bucket', metavar='<BUCKET>', type=str,
        required=True, help='target bucket name. *required param*'
    )
    argparser.add_argument('--filter_by_prefix', metavar='<FILTER>', type=str,
                           required=False,
                           default='',
                           help='filtering to get objects by key name prefix.\n' \
                                'Separates "," when you want to specified it more than two.'
    )
    argparser.add_argument('-L', '--loglevel', metavar='<LOGLEVEL>', type=int,
                           required=False, default=20,
                           help='loglevel of logger. default value is set to 20(INFO)\n' \
                           'a valid value 10 20 30 40 50\n' \
                           '10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL')
    argparser.add_argument('-H', '--handler', metavar='<HANDLER>', type=str,
                           required=False, default='rotation',
                           help='settings the handler of logging.\n' \
                           'default handler is "rotation".\n' \
                           'a valid value is file | console | rotation\n' \
                           'file: output in current directory\n' \
                           'console: output to standard out\n' \
                           'rotation: output in the <LOGPATH>'
    )
    argparser.add_argument('--logpath', metavar='<LOGPATH>', type=str,
                           required=False, default=LOGPATH_CLIENT,
                           help='if handler is rotation, logging in this parameter.\n' \
                           'default value is {0}'.format(LOGPATH_CLIENT)
    )

    args = argparser.parse_args()
    bucket = args.bucket
    fileter_by_prefixes = args.filter_by_prefix.split(",")
    loglevel = args.loglevel
    handler = args.handler
    logpath = r"{0}".format(args.logpath)
    result = dict()
    mail_body = 'Bucket: {}\n\n'.format(bucket)

    if os.path.isdir(logpath):
        os.makedirs(logpath, exist_ok=True)

    if handler == 'file':
        flogger_fac = FileLoggerFactory(logger_name=__name__,
                                        loglevel=loglevel)
        logger = flogger_fac.create(file=logpath)
        flogger_fac = FileLoggerFactory(logger_name=S3Client.__name__,
                                        loglevel=loglevel)
        logger_s3client = flogger_fac.create(file=LOGPATH_MODULE)
    elif handler == 'console':
        stdlogger_fac = StdoutLoggerFactory(logger_name=__name__,
                                            loglevel=loglevel)
        logger = stdlogger_fac.create()
    elif handler == 'rotation':
        rlogger_fac = RotationLoggerFactory(logger_name=__name__,
                                            loglevel=loglevel)
        logger = rlogger_fac.create(file=logpath,
                                    max_bytes=LOG_ROLLOVERSIZE,
                                    bcount=10)
        rlogger_fac = RotationLoggerFactory(logger_name=S3Client.__name__,
                                            loglevel=loglevel)
        logger_s3client = rlogger_fac.create(file=LOGPATH_MODULE,
                                             max_bytes=LOG_ROLLOVERSIZE,
                                             bcount=10)
    try:
        s3client = S3Client(bucket=bucket, logger=logger_s3client)
    except BotoCoreError as e:
        logger.exception('raised unexpected error while initializing s3 uploader client.')
        logger.error(e)
        raise e

    for prefix in fileter_by_prefixes:
        result = s3client.list_objs(prefix)

        for v in result.values():
            for d in v:
                mail_body += "Key: {0}, Size: {1} bytes\n".format(d["Key"], d["Size"])
        del result
    mail_body += "\nend\n"
    subject = 'Reports daily backup summaries at {0}'.format(time.strftime("%Y/%m/%d"))
    try:
        send_mail(subject=subject,
                body=mail_body,
                to_addr=TO_ADDR,
                cc_addr=CC_ADDR,
        )
    except smtplib.SMTPRecipientsRefused as smtp_refuse_e:
        recipients = [key for key in smtp_refuse_e.recipients.keys()]
        logger.error("The mail was not sent to {}".format(', '.join(recipients)))
    except smtplib.SMTPHeloError:
        logger.error("SMTP Server does not replied against HELO.")
    except smtplib.SMTPSenderRefused:
        logger.error("SMTP Server does not recieved from address.")
    except smtplib.SMTPDataError:
        logger.error("SMTP Server does responsed illegular error code.")
    except smtplib.SMTPNotSupportedError:
        logger.error("SMTP Server does not support 'SMTPUTF8'")
    except Exception as e:
        logger.error("Raise uncached error {}".format(e))
    else:
        logger.info("Send a report mail to {0}.".format(TO_ADDR))