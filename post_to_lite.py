import re
import sys
import sqlite3
from dateutil.parser import parse as date_parse

# todo:
#  look up in db schema the type for fields in table
#  use type to properly parse things like date

def main(argv):
    if len(argv) < 3:
        print("Usage:", argv[0], "OUTPUT POSTGRESQL_DUMP", file=sys.stderr)
        return 1

    output = argv[1]
    dump = argv[2]

    g = sqlite3.connect(output)
    f = open(dump)

    count = 0
    buf = str()
    for line in f:
        count += 1
        buf += line
        if sqlite3.complete_statement(buf):
            lines = buf.splitlines()
            lines = [x for x in lines if not x.startswith('--')]

            try:
                buf = buf.strip()
                g.execute(buf)
            except sqlite3.Error as e:
                g.commit()
                print("Failure:", count, buf)
                buf = '\n'.join(lines)


            if isa(buf, 'COPY '):
                if isa(buf, 'FROM STDIN;', True):
                    c = g.execute("SELECT name,sql FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' UNION ALL SELECT name,sql FROM sqlite_temp_master WHERE type IN ('table','view') ORDER BY 1")
                    tables = dict(c.fetchall())

                    tokens = tokenize(buf, '(),;')
                    name, fields = get_info(tokens)

                    schema = tables[name]
                    tokens = tokenize(schema, '(),;')
                    name, types = parse_schema(tokens)

                    count = insert_copies(f, g, name, types, len(fields), count)
                    print(count)
            buf = str()

    g.commit()

    f.close()
    g.close()

    return 0

def parse_schema(t):
    found = 0
    create_table = False
    table = None

    tmp = list()
    fields = list()
    for token in t:
        if token.upper() == 'CREATE' and table is None:
            table = ''
        elif token.upper() == 'TABLE' and table == '' and not create_table:
            create_table = True
        elif table == '' and create_table:
            table = token

        if token == '(':
            found = found + 1
        elif token == ')':
            found = found - 1
        elif token != ',' and found > 0:
            tmp.append(token)
        elif token == ',' and found > 0:
            n = tmp[0]
            ts = ' '.join(tmp[1:]).upper()
            # according to sqlite datatypes 2.1
            # https://www.sqlite.org/datatype3.html
            if 'INT' in ts:
                ty = int
            elif any([x in ts for x in ['CHAR', 'CLOB', 'TEXT']]):
                ty = str
            elif any([x in ts for x in ['REAL', 'FLOA', 'DOUB']]):
                ty = float
            # parse the date anyway
            elif ts == 'DATE' or ts == 'DATETIME':
                ty = date_parse
            else:
                ty = str # default to str in python instead of numeric,
                        # no real reason for this other than just cause

            fields.append((n, ty))
            tmp = list()

    return table, fields

def get_info(t):
    found = False
    fields = list()
    table = None
    for token in t:
        if token.upper() == 'COPY' and table is None:
            table = ''
        elif table == '':
            table = token

        if token == '(' and not found:
            found = True
        elif token == ')' and found:
            found = False
        elif token != ',' and found:
            fields.append(token)

    return table, fields

def isa(l, s, e=False):
    if e:
        return l.rstrip().upper().endswith(s)

    return l.lstrip().upper().startswith(s)

def tokenize(s, sep):
    tokens = list()
    build = str()
    for char in s:
        if char in sep:
            tokens.append(build)
            tokens.append(char)
            build = str()
        elif char.isspace():
            tokens.append(build)
            build = str()
        else:
            build += char

    return [x for x in tokens if x != '']


def insert_copies(f, g, table_name, table_schema, expecting, count):
    for line in f:
        count += 1
        if len(line.rstrip()) == 0:
            return count

        fields = line.rstrip().split('\t')
        if len(fields) != expecting:
            print(str(count)+": Number of fields ("+str(len(fields))+
                    ") do not match expected ("+str(expecting)+")")
            continue

        s = str()
        s += "INSERT INTO " + table_name + " VALUES ("
        s += "?, " * len(fields)
        s = s[:-2] + ");"

        for i in range(len(fields)):
            field = fields[i]
            t = table_schema[i][1]
            if field == "\\N":
                fields[i] = None
            elif t is not None:
                fields[i] = t(field)

        fields = tuple(fields)
        try:
            g.execute(s, fields)
        except sqlite3.Error as e:
            print("Failure:", count, s, fields, e)
        except Exception as e:
            print("Exception:", count, s, fields, e)
            sys.exit(1)

        if count % 100 == 0:
            g.commit()

    return count



if __name__ == '__main__':
    sys.exit(main(sys.argv))
