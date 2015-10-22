 # -*- coding: utf-8 -*-
__author__ = 'yuriy'

import psycopg2
import requests
import urllib
import random
import sys


SPELLER_ADDRESS = 'http://localhost:8080'
SPELLER_THRESHOLD = 50000
DATA_LIMIT = 1000

class Checker(object):
    def __init__(self, address=SPELLER_ADDRESS, threshold=SPELLER_THRESHOLD):
        self.address = address or SPELLER_ADDRESS
        self.threshold = threshold or SPELLER_THRESHOLD

    def check_spell(self, query):
        rate = 0
        fixed = ''
        unicoded = query
        if isinstance(query, unicode):
            unicoded = unicoded.encode('utf-8')
        encoded = urllib.quote(unicoded)
        url = self.address + "?query=" + encoded + '&format=json'
        response = requests.get(url)
        if response.ok:
            res = response.json()
            if 'correction' in res and 'rate' in res:
                rate = int(res['rate'])
                fixed = res['correction']
        return fixed, rate

    def check_server_fixed (self, fixed, rate):
        return fixed != '' and rate >= self.threshold

    def check_really_fixed(self, query, fixed):
        query = query.replace('ё', "е")
        return query.decode('utf-8').lower() == fixed.lower()

def stats(server_fixed, really_fixed, actual_len, fp = 0, fn = 0):
    res = 'analyzed: %s, server_fixed: %s (%.4f%%), really_fixed: %s (%.4f%%)' % ( actual_len, server_fixed, 100 * server_fixed/float(actual_len), really_fixed, 100*really_fixed/float(actual_len))
    if fp != 0 or fn != 0:
        res += ' ( false_positive: %s (%f%%), false_neg: %s (%f%%) )' % (fp, 100 * fp/float(actual_len), fn, 100*fn/float(actual_len))
    return res

class StatsItem(object):
    def __init__(self, checker):
        self.checker = checker or None
        self.server_fixed = 0
        self.really_fixed = 0
        self.falsepos = 0 #fixed, but wrong
        self.falseneg = 0 #says not fixed, but true

        self.cur_server_fixed = 0
        self.cur_really_fixed = 0
        self.cur_rate = 0
        self.fixed_query = ""

    def add_query(self, query, typo_query):
        self.cur_server_fixed = 0
        self.cur_really_fixed = 0

        self.cur_fixed_query, self.cur_rate = self.checker.check_spell(typo_query)
        fp = False
        fn = False
        if self.checker.check_server_fixed(self.cur_fixed_query, self.cur_rate):
            self.server_fixed += 1
            self.cur_server_fixed = 1
            fp = True
        else:
            fn = True
        if self.checker.check_really_fixed(query, self.cur_fixed_query):
            self.really_fixed += 1
            self.cur_really_fixed = 1
            fp &= False
            fn &= True
        else:
            fp &= True
            fn &= False

        if fp:
            self.falsepos += 1

        if fn:
            self.falseneg += 1

    def out_data(self):
        res = []
        res.append(self.cur_fixed_query.encode('utf-8'))
        res.append(str(self.cur_rate).encode('utf-8'))
        res.append(str(self.cur_server_fixed))
        res.append(str(self.cur_really_fixed))
        return res

class Stat(object):
    def __init__(self, checker, typo_maker, data_gen):
        self.typo_maker = typo_maker or None
        self.checker = checker or None
        self.data_gen = data_gen or None
        self.header = ['query', 'typo_query', 'fixed_query', 'treatment_rate', 'treatment_server_fixed', 'treatment_really_fixed', 'control_fixed_query', 'control_rate', 'control_server_fixed', 'control_really_fixed']

    def gather(self):
        treatment = StatsItem(self.checker)
        control = StatsItem(self.checker)
        actual_len = 0


        with open('out.txt', 'w') as out:
            out.write('\t'.join(self.header))
            out.write('\n')
            for query in self.data_gen:
                if len(query) == 0:
                    continue
                out_res = []
                typo_query = self.typo_maker.make_typo(query)
                treatment.add_query(query, typo_query)
                control.add_query(query, query)

                out_res.append(query)
                out_res.append(typo_query.encode('utf-8'))

                [out_res.append(d) for d in treatment.out_data()]
                [out_res.append(d) for d in control.out_data()]

                actual_len += 1
                sys.stdout.write("\r Treatment: %s" % stats(treatment.server_fixed, treatment.really_fixed, actual_len, treatment.falsepos, treatment.falseneg))
                sys.stdout.flush()
                out.write('\t'.join(out_res))
                out.write('\n')

        return treatment, control, actual_len

params = {
  'database': 'db',
  'user': 'user',
  'password': 'password',
  'host': 'host',
  'port': 8000
}

class DBConnector(object):
    def __init__(self):
        pass

    def prepare_data(self, pool, row):
        res = []
        columns = row.split(",")
        res.append(columns[-1].strip())
        for geo_obj in columns[:-1]:
            stripped_geo_obj = geo_obj.strip()
            if stripped_geo_obj not in pool:
                res.append(stripped_geo_obj)
                pool[stripped_geo_obj] = True
        return res

    def get_data(self):
        pool = {}
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                conn.set_client_encoding('UTF8')
                sel_statement = "select path from address limit %s;" % DATA_LIMIT
                cur.execute(sel_statement)

                while True:
                    row = cur.fetchone()
                    if not row or len(row) == 0:
                        break
                    columns = self.prepare_data(pool, row[0])
                    for column in columns:
                        yield column

class TypoMaker(object):
    def __init__(self):
        self.function_pool = [
            self.remove_space
            , self.add_space
            , self.remove_letter
            , self.add_letter
            , self.change_letter
                              ]
        self.alphabet = u'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'

    def make_typo(self, query):
        query = query.decode('utf-8')
        if len(query) == 0:
            return query
        maker = random.choice(self.function_pool)
        return maker(query)

    def remove_space(self, query):
        idx = query.find(u' ')
        if idx != -1:
            return query[:idx] + query[idx+1:]
        return query

    def add_space(self, query):
        idx = random.randrange(len(query))
        return query[:idx] + u' ' + query[idx:]

    def remove_letter(self, query):
        idx = random.randrange(len(query))
        return query[:idx] + query[idx+1:]

    def add_letter(self, query):
        idx = random.randrange(len(query))
        letter = self.random_letter()
        return query[:idx] + letter + query[idx:]

    def change_letter(self, query):
        idx = random.randrange(len(query))
        letter = self.random_letter()
        return query[:idx] + letter + query[idx+1:]

    def random_letter(self):
        idx = random.randrange(len(self.alphabet))
        return self.alphabet[idx]


if __name__ == '__main__':
    connector = DBConnector()
    data = connector.get_data()

    typo_maker = TypoMaker()
    checker = Checker()
    statistics = Stat(checker, typo_maker, data)
    treatment, control, actual_len = statistics.gather()
    stat_treatment = stats(treatment.server_fixed, treatment.really_fixed, actual_len, treatment.falsepos, treatment.falseneg)
    stat_control = stats(control.server_fixed, control.really_fixed, actual_len, control.falsepos, control.falseneg)
    print
    print "Treatment: %s" % stat_treatment
    print "Control: %s" % stat_control
    with open('stats.txt', 'w') as out:
        out.write("Treatment: ")
        out.write(stat_treatment)
        out.write('\n')
        out.write("Control: ")
        out.write(stat_control)
        out.write('\n')
    print 'Done'
