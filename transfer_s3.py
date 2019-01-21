#!/bin/python3
# -*- coding: utf-8 -*-
from mylogger.factory import StdoutLoggerFactory, \
                             FileLoggerFactory, \
                             RotationLoggerFactory

import os
import time
import tarfile
import shutil
import argparse
import smtplib
import configparser
from botocore.exceptions import BotoCoreError
from s3_client.s3_client import S3Uploader
from socket import gethostname
from email.mime.text import MIMEText

CONF_FILE = 'transfer_s3.ini'
ARCHIVE_MODE = 'w:gz'
SMTP_SERVER = '59.128.93.227'
MULTIPART_THRESHOLD = 8 * 1024 * 1024

SUBJECT_SUCCESS = '[SUCCESS] {} DB Backup notification'.format(time.strftime('%Y/%m/%d'))
SUBJECT_FAILED = '[FAILED] {} DB Backup notification'.format(time.strftime('%Y/%m/%d'))


if __name__ == '__main__':

    def send_mail(bucket: str,
                  src_path: str,
                  key_name: str,
                  to_addr: str,
                  cc_addr: str,
                  smtp_server=None,
                  subject=None,
                  filesize=0):
        if subject is None:
            subject = '{} DB Backup notification'.format(time.strftime('%Y/%m/%d'))
        if smtp_server is None:
            smtp_server = SMTP_SERVER
        from_addr = '{0}@local'.format(gethostname())
        body = 'Amazon S3 uploading notification.\n' \
               'Bucket: {0}\n' \
               'SourceFilePath: {1}\n' \
               'KeyName: {2}\n' \
               'FileSize(Bytes): {3}\n\n' \
               'if FileSize(Bytes) >= {4}, selects MultiPartUpload.' \
               .format(bucket, src_path, key_name, filesize, MULTIPART_THRESHOLD)
        
        mail = MIMEText(body)
        mail['To'] = to_addr
        mail['Cc'] = cc_addr
        mail['From'] = from_addr
        mail['Subject'] = subject

        server = smtplib.SMTP(smtp_server)
        try:
            server.ehlo()
            server.sendmail(from_addr, [[to_addr], [cc_addr]], mail.as_string())
        finally:
            server.quit()

    description = 'Transfer a specified file/dir to amazon S3.'
    argparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                        description=description)
    argparser.add_argument('-b', '--bucket', metavar='<BUCKET>', type=str,
                           required=True,
                           help='Target Bucket name. *required param')                                    
    argparser.add_argument('-s', '--src_path', metavar='<SRC_PATH>', type=str,
                           required=True,
                           help='target file/dir path. *required param')
    argparser.add_argument('--aws_cred_secname', metavar='<AWS_CRED>', type=str,
                           required=False,
                           default=None,
                           help='aws credentials(access key/secret code) to use.\n' \
                                'please specify section name of ~/.aws/credentials')                       
    argparser.add_argument('-k', '--key_name', metavar='<KEY_NAME>', type=str,
                           required=False,
                           default=None,
                           help='Key name on amazon s3. dafault value is None.\n' \
                                 'The file will be uploaded root by default.')
    argparser.add_argument('--no_compress', action='store_true',
                           required=False,
                           help='not compress the target file.')
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
                           'rotation: output in the <LOGPATH>')
    argparser.add_argument('-c', '--config', metavar='<CONFIG>', type=str,
                           required=False,
                           default=CONF_FILE,
                           help='config file path. loading config in current directory by default'
                           )
    args = argparser.parse_args()

    ###### Parse config file. ######
    config = configparser.ConfigParser()
    config.read(args.config)
    conf_general = config['GENERAL']
    conf_logging = config['Logging']
    conf_mail = config['Mail']

    ###### Set variables ######
    # s3 bucket name
    bucket = args.bucket
    # source file path puts s3 bucket
    src_path = r"{0}".format(args.src_path)
    # section name in /~/.aws/credentials
    aws_cred_secname = args.aws_cred_secname
    # key name of s3 object 
    key_name = args.key_name
    # flag whether compress source file
    is_nocomp = args.no_compress
    # Logging settings
    loglevel = args.loglevel
    handler = args.handler
    logpath = conf_logging['log_path']
    log_rolloversize = int(conf_logging['log_rolloversize'])
    # Mail settings
    smtp_server = conf_mail['smtp_server']
    to_addr = conf_mail['to_address']
    cc_addr = conf_mail['cc_address']
    # config file path
    conf_path = args.config
    filesize = 0

    if os.path.isdir(logpath):
        os.makedirs(logpath, exist_ok=True)

    if handler == 'file':
        flogger_fac = FileLoggerFactory(logger_name=__name__,
                                        loglevel=loglevel)
        logger = flogger_fac.create(file=logpath)
    elif handler == 'console':
        stdlogger_fac = StdoutLoggerFactory(logger_name=__name__,
                                            loglevel=loglevel)
        logger = stdlogger_fac.create()
    elif handler == 'rotation':
        rlogger_fac = RotationLoggerFactory(logger_name=__name__,
                                            loglevel=loglevel)
        logger = rlogger_fac.create(file=logpath,
                                    max_bytes=log_rolloversize,
                                    bcount=10)

    ###### start upload process to s3 bucket ######
    logger.info('Start Uploading to S3 {0} bucket'.format(bucket))
    try:
        client = S3Uploader(bucket, aws_cred_secname=aws_cred_secname)
    except BotoCoreError as e:
        logger.exception('raised unexpected error while initializing s3 uploader client.')
        logger.error(e)
        send_mail(bucket, src_path, key_name, to_addr, cc_addr, smtp_server=smtp_server, subject=SUBJECT_FAILED)
        raise e

    ###### archiving tar file ######
    archive_name = src_path
    if not is_nocomp:
        archive_name = r"{0}.tar.gz".format(src_path)
        try:
            logger.info('start to creates archive file {0}'.format(archive_name))
            with tarfile.open(archive_name, ARCHIVE_MODE) as tar:
                tar.add(src_path)
        except FileNotFoundError as notfound_e:
            logger.error('{0} not found.'.format(src_path))
            send_mail(bucket, src_path, key_name, to_addr, cc_addr, smtp_server=smtp_server, subject=SUBJECT_FAILED)
            raise notfound_e
        except tarfile.TarError as tar_e:
            logger.error('raised unexpected error {0}'.format(tar_e))
            send_mail(bucket, src_path, key_name, to_addr, cc_addr, smtp_server=smtp_server, subject=SUBJECT_FAILED)
            raise tar_e
        else:
            logger.info('created archive file {0}'.format(archive_name))
    filesize = os.path.getsize(archive_name)

    ###### upload tar file to s3 bucket. ######
    logger.info('Start uploading {0} to amazon s3. ' \
                'uploading status is logging to /var/log/S3Operation.log'.format(archive_name))
    try:
        client.upload(archive_name, key_name=key_name)
    except BotoCoreError as e:
        logger.error('raised unexpected error while uploading process.')
        logger.error(e)
        send_mail(bucket, archive_name, key_name, to_addr, cc_addr, smtp_server=smtp_server, subject=SUBJECT_FAILED, filesize=filesize)
        raise e
    else:
        logger.info('complete uploading {0} to {1}'.format(archive_name, bucket))
    finally:
        if not is_nocomp:
            os.remove(archive_name)
            logger.info('Removed archive file {0}'.format(archive_name))

    ###### send mail ######
    try:
        send_mail(bucket, archive_name, key_name, to_addr, cc_addr, smtp_server=smtp_server, subject=SUBJECT_SUCCESS)
    except Exception as e:
        logger.exception('Failed to send a mail. {0}'.format(str(e)))
    else:
        logger.info("Send a mail to {0}".format(to_addr))

