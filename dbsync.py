#!/usr/bin/env python3

import json
import os
import sys
from subprocess import check_output

import psycopg2

SOURCE_DB = os.environ['SOURCE_DB']
TARGET_DB = os.environ['TARGET_DB']
ROW_LIMIT = os.environ.get('ROW_LIMIT', 10000)
SAMPLED_TABLES = set(os.environ['SAMPLED_TABLES'].split(',')) if 'SAMPLED_TABLES' in os.environ else set()
SKIPPED_TABLES = set(os.environ['SKIPPED_TABLES'].split(','))  if 'SKIPPED_TABLES' in os.environ else set()

BEGIN_DUMP = '''
--
-- PostgreSQL database dump
--

\connect {dbname}
\set AUTOCOMMIT off

BEGIN;

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;
'''

END_DUMP = '''
COMMIT;

--
-- PostgreSQL database dump complete
--
'''

LIST_TABLES_SQL = """SELECT table_name
FROM information_schema.tables
WHERE
table_catalog = %s
AND table_schema = 'public'
AND table_type = 'BASE TABLE'
AND table_name NOT IN %s"""

GET_COLUMNS_SQL = """SELECT a.attname as colname, format_type(a.atttypid, a.atttypmod) as coltype
FROM pg_catalog.pg_attribute a
WHERE
        attrelid = %s::regclass
        AND attnum > 0
        AND attisdropped = FALSE
        ORDER BY attnum"""

GET_FK_CONSTRAINTS_SQL = """SELECT conname,
  pg_catalog.pg_get_constraintdef(r.oid, true) as condef
FROM pg_catalog.pg_constraint r
WHERE r.conrelid = %s::regclass AND r.contype = 'f'"""

BEGIN_TABLE_DUMP = '''
--
-- Data for Name: {table}; Type: TABLE DATA
--

COPY public."{table}" ({columns}) FROM STDIN;
'''

END_TABLE_DUMP = '''\.
'''


def get_tables(cursor, dbname, sampled_tables, row_limit, skipped_tables=None):
    skipped_tables = skipped_tables.union(sampled_tables) or sampled_tables
    cursor.execute(LIST_TABLES_SQL, (dbname, tuple(skipped_tables)))
    rows = cursor.fetchall()

    tables = {Table(t[0], cursor) for t in rows}

    sampled_tables = {Table(t, cursor, limit=row_limit) for t in sampled_tables}
    return tables, sampled_tables


class FkConstraint:
    def __init__(self, table, name, condef):
        self.table = table
        self.name = name
        self.condef = condef

    @property
    def drop(self):
        return 'ALTER TABLE ONLY public."{}" DROP CONSTRAINT IF EXISTS {};\n'.format(self.table, self.name)

    @property
    def create(self):
        return 'ALTER TABLE ONLY public."{}" ADD CONSTRAINT {} {};\n'.format(self.table, self.name, self.condef)


class Table:
    def __init__(self, name, cursor, limit=None):
        self.name = name
        self.cursor = cursor
        self.limit = limit
        self.ids = None
        self._foreign_tables = {}
        self._columns = None
        self._coltypes = None
        self._foreign_keys = None
        self._ididx = None

    @property
    def columns(self):
        if self._columns is None:
            self.cursor.execute(GET_COLUMNS_SQL, (self.name,))
            rows = self.cursor.fetchall()
            self._columns = [r[0] for r in rows]
            self._coltypes = [r[1] for r in rows]

        return self._columns

    @property
    def foreign_key_constraints(self):
        if self._foreign_keys is None:
            self.cursor.execute(GET_FK_CONSTRAINTS_SQL, (self.name,))
            rows = self.cursor.fetchall()
            self._foreign_keys = [FkConstraint(self.name, row[0], row[1]) for row in rows]

        return self._foreign_keys

    @property
    def ididx(self):
        if self._ididx is None:
            self._ididx = 0
            for c in self.columns:
                if c == 'id':
                    return self._ididx
                self._ididx += 1
        return self._ididx

    def dump(self, outfile):
        columns = ','.join(['"{}"'.format(c) for c in self.columns])

        if self.limit is None:
            self._copy(outfile, columns)
        else:
            self.ids = self._select_and_insert(outfile, columns)

    def drop_foreign_key_constraints(self, outfile=sys.stdout):
        for fk in self.foreign_key_constraints:
            outfile.write(fk.drop)
        outfile.write('\n')

    def create_foreign_key_constraints(self, outfile=sys.stdout):
        for fk in self.foreign_key_constraints:
            outfile.write(fk.create)
        outfile.write('\n')

    def add_foreign_table(self, name, ids):
        if name in self._foreign_tables:
            raise RuntimeError('no bueno')

        self._foreign_tables[name] = tuple(ids)

    def _copy(self, outfile, columns):
        outfile.write(BEGIN_TABLE_DUMP.format(table=self.name, columns=columns))

        sql = 'COPY (SELECT {columns} FROM public."{table}" {where}) TO STDOUT'.format(table=self.name, columns=columns,
                                                                                       where=self._build_where_clause())
        self.cursor.copy_expert(sql, outfile)
        outfile.write(END_TABLE_DUMP)

    def _build_where_clause(self):
        where = []

        for ftable, fids in self._foreign_tables.items():
            where.append('{foreign_table}_id in {ids}'.format(foreign_table=ftable, ids=fids))

        return 'WHERE {}'.format(' AND '.join(where)) if len(where) > 0 else ''

    def _select_and_insert(self, outfile, columns):
        outfile.write('INSERT INTO {table} ({columns}) VALUES \n'.format(table=self.name, columns=columns))
        sql = 'SELECT {columns} FROM public."{table}" LIMIT {limit}'.format(table=self.name, columns=columns, limit=self.limit)
        self.cursor.execute(sql)

        ids = []
        rows_written = 1
        for row in self.cursor:
            ids.append(row[self.ididx])
            outfile.write(self._mogrify_row(row))

            if rows_written == self.cursor.rowcount:
                outfile.write(';\n')
            else:
                outfile.write(',\n')

            rows_written += 1

        outfile.write('\n')
        return ids

    def _mogrify_row(self, row):
        strrow = [self._stringify_value(v, i) for i, v in enumerate(row)]
        return '({})'.format(','.join(strrow))

    def _stringify_value(self, value, colidx):
        if value is None: return 'NULL'

        coltype = self._coltypes[colidx]
        fmtstr = "{}"

        if coltype.startswith('character') or coltype.startswith('geography') or coltype.startswith('json'):
            fmtstr = "'{}'"

            if coltype.startswith('json'):
                value = json.dumps(value)

        if coltype.startswith('timestamp'):
            fmtstr = "'{}'"
            value = value.isoformat()

        return fmtstr.format(value)


def set_sampled_ids(sampled_tables, full_tables):
    '''
    Find the tables that have a foreign key to a sampled table t, and restrict their dump to the rows we dumped from t.
    '''
    for sampled_table in sampled_tables:
        fkey_colname = sampled_table.name + '_id'

        for t in full_tables:
            if fkey_colname in t.columns:
                t.add_foreign_table(sampled_table.name, sampled_table.ids)


def sync_schema(source, target):
    cmd = 'pg_dump --clean --create --no-owner --no-acl --format=p --schema-only {} | psql -q {}'.format(source, target)
    return check_output(cmd, shell=True)


def main(outfile):
    sync_schema(SOURCE_DB, TARGET_DB)

    with psycopg2.connect(SOURCE_DB) as pgcon:
        dbname = pgcon.get_dsn_parameters()['dbname']
        outfile.write(BEGIN_DUMP.format(dbname=dbname))

        with pgcon.cursor() as cursor:
            full_tables, sampled_tables = get_tables(cursor, dbname, SAMPLED_TABLES, ROW_LIMIT, skipped_tables=SKIPPED_TABLES)
            tables = full_tables.union(sampled_tables)

            for t in tables:
                t.drop_foreign_key_constraints(outfile)

            for t in sampled_tables:
                t.dump(outfile)

            set_sampled_ids(sampled_tables, full_tables)

            for t in full_tables:
                t.dump(outfile)

            for t in tables:
                t.create_foreign_key_constraints(outfile)

            outfile.write(END_DUMP)


if __name__ == '__main__':
    main(sys.stdout)
