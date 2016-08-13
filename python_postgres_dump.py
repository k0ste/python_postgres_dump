#!/usr/bin/python -u
# -*- coding: utf-8 -*-

"""
GPL
2016, (c) Konstantin Shalygin <k0ste@k0ste.ru>
"""

import subprocess
import os,sys
import shutil
import json
import logging
from logging.handlers import TimedRotatingFileHandler
from optparse import OptionParser

class PostgresCommand(object):
    def get_all_databases(self):
        """
        Method return all databases in PostgreSQL instance.
        """

        self.cmd = [self.pg_psql]
        self.cmd.append("-A") # No align for output without separators
        self.cmd.append("-q") # No welcome messages, row counters
        self.cmd.append("-t") # No column names
        self.cmd.extend(["-F", " "]) # Field separator

        pg_query = """
                   SELECT datname FROM pg_database;
                   """

        self.cmd.extend(["-h", self.pg_host,
                    "-p", self.pg_port,
                    "-U", self.pg_user,
                    "-d", self.pg_db,
                    "-c", pg_query])

        proc = subprocess.Popen(self.cmd, env={"PGPASSWORD":self.postgres_password},
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True)

        out, err = proc.communicate()
        rc = proc.returncode

        if rc == 0:
            logging.info("Receive all databases from host '{0}:{1}'.".format(self.pg_host, self.pg_port))
            return out
        else:
            raise Exception(err)

    def worker(self):
        """
        For all databases in PostgreSQL instance:

        1. Check db state in json-file. If excluded - do nothing.
        2. Backup databases.
        3. Backup globals.
        """

        logging.info("Start worker.")

        pg_dbs = self.get_all_databases()

        for db in pg_dbs.splitlines():
            s = self.check_database_state(db)
            if s:
                try:
                    cmd = self.make_backup_cmd(db)
                    self.backup_single_db(db, cmd)
                except:
                    logging.error("Can't backup database '{0}'.".format(db))
            else: pass

        g = self.backup_globals() # Always backup globals
        if g:
            logging.info("Stop worker.")

    def make_backup_cmd(self, db):
        """
        Method return cmd with PostgreSQL options and compressor options.
        """

        cmd = [self.pg_dump, "-F", "c", "-b"] # Format: custom. Blobs.
        cmd.extend(["-h", self.pg_host,
                    "-p", self.pg_port,
                    "-U", self.pg_user])

        extra_cmd = self.parser(db)
        if extra_cmd:
            cmd.extend(extra_cmd)

        cmd.append(db)

        if self.comp == "gzip":
            cmd.extend(["|", self.comp_path, "-c", "-{0}".format(self.level), ">", "{0}/{1}.tar.gz".format(self.output, db)])
        elif self.comp == "7z" or self.comp == "7za":
            cmd.extend(["|", self.comp_path, "a", "-si", "-mx={0}".format(self.level), "{0}/{1}.tar.7z".format(self.output, db)])
        elif self.comp == "xz" or self.comp == "lzma":
            cmd.extend(["|", self.comp_path, "-zfc", "-{0}".format(self.level), ">", "{0}/{1}.tar.xz".format(self.output, db)])

        return cmd

    def backup_single_db(self, db, cmd):
        """
        Backup singe database via generated cmd.
        """

        logging.error("Start backup database '{0}'.".format(db))

        proc = subprocess.Popen(' '.join(cmd), env={"PGPASSWORD":self.postgres_password},
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    shell=True)

        out, err = proc.communicate()
        rc = proc.returncode

        if rc == 0:
            logging.info("OK backup database '{0}'.".format(db))
            return True
        else:
            raise Exception(err)
            return

    def backup_globals(self):
        """
        Backup PostgreSQL globals via pg_dumpall.
        """

        logging.info("Start backup globals.")

        cmd = [self.pg_dumpall, "-g", "-h", self.pg_host, "-p", self.pg_port, "-U", self.pg_user]

        if self.comp == "gzip":
            cmd.extend(["|", self.comp_path, "-c", "-{0}".format(self.level), ">", "{0}/globals.sql.gz".format(self.output)])
        elif self.comp == "7z" or self.comp == "7za":
            cmd.extend(["|", self.comp_path, "a", "-si", "-mx={0}".format(self.level), "{0}/globals.sql.7z".format(self.output)])
        elif self.comp == "xz" or self.comp == "lzma":
            cmd.extend(["|", self.comp_path, "-zfc", "-{0}".format(self.level), ">", "{0}/globals.sql.xz".format(self.output)])

        proc = subprocess.Popen(' '.join(cmd), env={"PGPASSWORD":self.postgres_password},
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                shell=True)

        out, err = proc.communicate()
        rc = proc.returncode

        if rc == 0:
            logging.info("OK backup globals.")
            return True
        else:
            raise Exception(err)
            return

    def check_database_state(self, db):
        """
        Exclude disabled databases (like template0 and any other
        defined in json).
        """

        for database_index in range(len(self.json_root)):

            db_name = self.json_root[database_index]["name"]
            db_state = self.json_root[database_index]["state"]

            if db_name == db and db_state == "disabled":
                logging.info("Database '{0}' disabled for dump.".format(db))
                return False

        return True

    def parser(self, db):
        """
        Parse json-file for worker(). If database found, parse for
        exclude-schema definitions.
        """

        cmd = []

        for database_index in range(len(self.json_root)):

            if self.json_root[database_index]["name"] == db:
                for schema_index in range(len(self.json_root[database_index]["schema"])):
                    json_schema_name = self.json_root[database_index]["schema"][schema_index]["name"]
                    json_schema_state = self.json_root[database_index]["schema"][schema_index]["state"]

                    cmd.extend(['-N', json_schema_name])

            return cmd

    def logger(self, log_file=None):
        """
        Console Handler - for output to stdout.
        File Handler - to file with rotation.
        """
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        log_formatter = logging.Formatter(fmt="[%(asctime)s] %(message)s",
                                          datefmt="%a %b %d %H:%M:%S %Z %Y") # Date in Linux format

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter)
        root_logger.addHandler(console_handler)

        if log_file:
            log_file = "{0}/{1}.log".format(log_file, os.path.splitext(sys.argv[0])[0])
            file_handler = TimedRotatingFileHandler(log_file, "midnight", backupCount=7)
            file_handler.setFormatter(log_formatter)
            root_logger.addHandler(file_handler)

    def __init__(self):
        parser = OptionParser(usage="%prog -H localhost -p secret -o /opt/backups --log /var/log/backups", version="%prog 0.3")
        parser.add_option("-c", "--compressor", type="choice", dest="comp", default="gzip", choices=['gzip', 'xz', 'lzma', '7z', '7za'], help="Compressor (7z, gzip, xz) [default: %default]")
        parser.add_option("-l", "--level", type="int", dest="level", default="9", help="Compression level (range 1-9) [default: %default]")
        parser.add_option("-o", "--output", type="string", dest="output", help="Output path [default: %default]")
        parser.add_option("-j", "--json-file", type="string", dest="json_file", default="pg_db.json", help="JSON file [default: %default]")
        parser.add_option("-H", "--host", type="string", dest="pg_host", help="PostgreSQL host [default: %default]")
        parser.add_option("-P", "--port", type="int", dest="pg_port", default="5432", help="PostgreSQL port [default: %default]")
        parser.add_option("-d", "--database", type="string", dest="pg_db", default="postgres", help="PostgreSQL database [default: %default]")
        parser.add_option("-u", "--user", type="string", dest="pg_user", default="postgres", help="PostgreSQL user [default: %default]")
        parser.add_option("-p", "--password", type="string", dest="postgres_password", help="PostgreSQL password [default: %default]")
        parser.add_option("-L", "--log", type="string", dest="log_file", help="Where save log [default: %default]")
        (opts, args) = parser.parse_args()

        if (not opts.pg_host or not opts.postgres_password or not opts.output):
            parser.print_help()
            sys.exit(1)

        if opts.level >= 1 and opts.level <= 9:
            self.level = str(opts.level)
        else:
            parser.error("\nCompression level must be in range 1-9, now is '{0}'.".format(opts.level))

        if not shutil.which(opts.comp):
            parser.error("\nUnable to find '{0}' in '{1}'.".format(opts.comp, os.defpath))
        else:
            self.comp = opts.comp
            self.comp_path = shutil.which(opts.comp)

        if not os.path.isfile(opts.json_file):
            parser.error("\nUnable to open JSON '{0}'.".format(opts.json_file))

        if not shutil.which("psql"):
            parser.error("\nUnable to find psql in '{0}'.".format(os.defpath))
        else: self.pg_psql = shutil.which("psql")

        if not shutil.which("pg_dump"):
            parser.error("\nUnable to find pg_dump in '{0}'.".format(os.defpath))
        else: self.pg_dump = shutil.which("pg_dump")

        if not shutil.which("pg_dumpall"):
            parser.error("\nUnable to find pg_dumpall in '{0}'.".format(os.defpath))
        else: self.pg_dumpall = shutil.which("pg_dumpall")

        if not os.path.isdir(opts.output):
            try:
                os.makedirs(opts.output)
            except OSError as e:
                raise OSError(e)

        if opts.log_file and not os.path.isdir(opts.log_file):
            try:
                os.makedirs(opts.log_file)
            except OSError as e:
                raise OSError(e)

        with open(opts.json_file) as json_data:
            self.json_full = json.loads(json_data.read())
            self.json_root = self.json_full["database"]

        self.logger(opts.log_file)
        self.output = opts.output
        self.pg_host = opts.pg_host
        self.pg_port = str(opts.pg_port)
        self.pg_db = opts.pg_db
        self.pg_user = opts.pg_user
        self.postgres_password = opts.postgres_password

def main():
    postgres_command = PostgresCommand()
    postgres_command.worker()

if __name__ == "__main__":
    main()
