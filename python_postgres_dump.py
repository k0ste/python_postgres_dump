#!/usr/bin/python -u
# -*- coding: utf-8 -*-

"""
GPL
2016, (c) Konstantin Shalygin <k0ste@k0ste.ru>
version: 0.1
"""

import subprocess
import os,sys,shutil
import json
from optparse import OptionParser
from datetime import datetime, timezone

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
                                universal_newlines=True, shell=False)

        out, err = proc.communicate()
        rc = proc.returncode

        if rc == 0:
            sys.stdout.write("[{0}] Receive all databases from host '{1}:{2}'.\n".format(self.now, self.pg_host, self.pg_port))
            return out
        else:
            raise Exception(err)

    def worker(self):
        """
        For all databases in PostgreSQL instance:

        1. Check db state in JSON. If excluded - do nothing.
        2. Backup databases.
        3. Backup globals.
        """
        sys.stdout.write("[{0}] Start worker.\n".format(self.now))

        pg_dbs = self.get_all_databases()

        for db in pg_dbs.splitlines():
            s = self.check_database_state(db)
            if s:
                try:
                    cmd = self.make_backup_cmd(db)
                    self.backup_single_db(db, cmd)
                except:
                    sys.stderr.write("[{0}] Can't backup database '{1}'.\n".format(self.now, db))
            else: pass

        g = self.backup_globals() # Always backup globals
        if g:
            sys.stdout.write("[{0}] Stop worker.\n".format(self.now))

    def make_backup_cmd(self, db):
        """
        Method return cmd with PostgreSQL options and compressor options.
        """

        cmd = [self.pg_dump, "-F", "c", "-b"] # provided by Krapchatov Iliya
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
            cmd.extend(["|", "/usr/bin/xz", "-zfc", "-{0}".format(self.level), ">", "{0}/{1}.tar.xz".format(self.output, db)])

        return cmd

    def backup_single_db(self, db, cmd):
        """
        Backup singe database via generated cmd.
        """
        sys.stdout.write("[{0}] Start backup database '{1}'.\n".format(self.now, db))

        proc = subprocess.Popen(' '.join(cmd), env={"PGPASSWORD":self.postgres_password},
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    universal_newlines=False, shell=True)

        out, err = proc.communicate()
        rc = proc.returncode

        if rc == 0:
            sys.stdout.write("[{0}] OK backup database {1}.\n".format(self.now, db))
            return True
        else:
            raise Exception(err)
            return

    def backup_globals(self):
        """
        Backup PostgreSQL globals via pg_dumpall.
        """
        sys.stdout.write("[{0}] Start backup globals.\n".format(self.now))

        cmd = [self.pg_dumpall, "-g", "-h", self.pg_host, "-p", self.pg_port, "-U", self.pg_user]

        if self.comp == "gzip":
            cmd.extend(["|", self.comp_path, "-c", "-{0}".format(self.level), ">", "{0}/globals.sql.gz".format(self.output)])
        elif self.comp == "7z" or self.comp == "7za":
            cmd.extend(["|", self.comp_path, "a", "-si", "-mx={0}".format(self.level), "{0}/globals.sql.7z".format(self.output)])
        elif self.comp == "xz" or self.comp == "lzma":
            cmd.extend(["|", "/usr/bin/xz", "-zfc", "-{0}".format(self.level), ">", "{0}/globals.sql.xz".format(self.output)])

        proc = subprocess.Popen(' '.join(cmd), env={"PGPASSWORD":self.postgres_password},
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=False, shell=True)

        out, err = proc.communicate()
        rc = proc.returncode

        if rc == 0:
            sys.stdout.write("[{0}] OK backup globals.\n".format(self.now))
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
                sys.stdout.write("[{0}] Database '{1}' will not be dumped.\n".format(self.now, db))
                return False

        return True

    def parser(self, db):
        """
        Parse json-file for worker(). If database found, json parse for
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

    def __init__(self):
        parser = OptionParser(usage="%prog -H localhost -p secret -o /opt/backups   ", version="%prog 0.1")
        parser.add_option("-c", "--compressor", type="choice", dest="comp", default="gzip", choices=['gzip', 'xz', 'lzma', '7z', '7za'], help="Compressor (7z, gzip, xz) [default: %default]")
        parser.add_option("-l", "--level", type="int", dest="level", default="9", help="Compression level (range 1-9) [default: %default]")
        parser.add_option("-o", "--output", type="string", dest="output", default="", help="Output path [default: %default]")
        parser.add_option("-j", "--json-file", type="string", dest="json_file", default="pg_db.json", help="JSON file for exclude schemas [default: %default]")
        parser.add_option("-H", "--host", type="string", dest="pg_host", help="PostgreSQL host [default: %default]")
        parser.add_option("-P", "--port", type="int", dest="pg_port", default="5432", help="PostgreSQL port [default: %default]")
        parser.add_option("-d", "--database", type="string", dest="pg_db", default="postgres", help="PostgreSQL database [default: %default]")
        parser.add_option("-u", "--user", type="string", dest="pg_user", default="postgres", help="PostgreSQL user [default: %default]")
        parser.add_option("-p", "--password", type="string", dest="postgres_password", help="PostgreSQL password [default: %default]")
        (opts, args) = parser.parse_args()

        if (not opts.pg_host or not opts.postgres_password or not opts.output):
            parser.print_help()
            sys.exit(1)

        if opts.level >= 1 and opts.level <= 9:
            self.level = str(opts.level)
        else:
            parser.error("\nCompression level must be in range 1-9, now is '{0}'\n".format(opts.level))

        if not shutil.which(opts.comp):
            parser.error("\nUnable to find: {0} in '{1}'\n".format(opts.comp, os.defpath))
        else:
            self.comp = opts.comp
            self.comp_path = shutil.which(opts.comp)

        if not os.path.isdir(opts.output):
            try:
                os.makedirs(opts.output)
            except OSError as e:
                raise OSError(e)

        if not os.path.isfile(opts.json_file):
            parser.error("\nUnable to open: {0}\n".format(opts.json_file))

        if not shutil.which("psql"):
            sys.stderr.write("\nUnable to find: psql in '{0}'\n".format(os.defpath))
        else: self.pg_psql = shutil.which("psql")

        if not shutil.which("pg_dump"):
            sys.stderr.write("\nUnable to find: pg_dump in '{0}'\n".format(os.defpath))
        else: self.pg_dump = shutil.which("pg_dump")

        if not shutil.which("pg_dumpall"):
            sys.stderr.write("\nUnable to find: pg_dumpall in '{0}'\n".format(os.defpath))
        else: self.pg_dumpall = shutil.which("pg_dumpall")

        with open(opts.json_file) as json_data:
            self.json_full = json.loads(json_data.read())
            self.json_root = self.json_full["database"]

        self.now = datetime.now(timezone.utc).astimezone().strftime("%c %z") # Date in Linux date format
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
