# -*- coding: utf-8 -*-
"""
Convert prakriya data to tsv format in a pivoted format

@author: alvarna
"""

from __future__ import print_function
import os.path
import json
import tarfile
import time
import logging
import codecs
from itertools import product
from collections import defaultdict

from indic_transliteration import sanscript
logger = logging.getLogger(__name__)

def to_devanagari(s):
    return sanscript.transliterate(s, sanscript.SLP1, sanscript.DEVANAGARI)

def get_full_data(fin):
    """Get whole data from the json file for given verb form."""
    data = json.load(fin)
    result = []
    for datum in data:
        subresult = {}
        for item in datum:
            if item in ['gana', 'padadecider_id', 'padadecider_sutra', 
                        'meaning', 'verb', 'it_status', 'it_sutra', 
                        'purusha', 'vachana', 'upasarga']:
                tmp = datum[item]
                if item == 'verb' or item == 'meaning':
                    tmp = tmp.replace('!', '~')
                subresult[item] = to_devanagari(tmp)
            elif item == 'derivation':
                pass
            else:
                subresult[item] = datum[item]
        for member in datum['derivation']:
            if member['sutra_num'] == "3.4.69":
                subresult["dhAtu"] = to_devanagari(member['form'])
                break
        result.append(subresult)
    return result

lakAras = ['law', 'liw', 'luw', 'lfw', 'low', 
           'laN', 'viDiliN', 'ASIrliN', 'luN', 'lfN']

suffixes = ['tip', 'tas', 'Ji', 'sip', 'tas', 'Ta', 'mip', 'vas', 'mas',
            'ta', 'AtAm', 'Ja', 'TAs', 'ATAm', 'Dvam', 'iw', 'vahi', 'mahiN']

headers = ["धातुः", "धातुपाठे", "अर्थः", "गणः", "संख्या"]

lakAras_dev = map(to_devanagari, lakAras)
puruSha = ["प्र", "म", "उ"]
vachana = ["एक", "द्वि", "बहु"]
pada = ["परस्मै", "आत्मने"]
headers.extend("-".join(list(x[1]) + [x[0]]) \
               for x in product(pada, product(lakAras_dev, puruSha, vachana)))

def to_tsv_row(details, forms):
    s = "\t".join(details)
    for x in product(lakAras, suffixes):
        s += "\t" + ",".join(forms[x])
    s += "\n"
    return s

if __name__ == '__main__':
    start = time.time()
    logging.basicConfig(level=logging.INFO)
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    forms_file = os.path.join(data_dir, "derivation_v003.tar.gz")
    out_file = os.path.join(data_dir, "dhaval_verb_forms.tsv")
    logger.info("Reading from tar file of forms and generating csv ...")
    
    logger.info("Indexing forms")
    dhAtu_details = {} # dhAtu number -> details
    fn = lambda: defaultdict(list)
    dhAtu_forms = defaultdict(fn) # dhAtu -> forms
    with tarfile.open(forms_file, 'r') as tf:
        files = tf.getmembers()
        for i, file in enumerate(files):
            if not file.isfile(): 
                continue
            f = tf.extractfile(file)
            prakriya = get_full_data(f)
            form = os.path.splitext(os.path.basename(file.name))[0]
            form = to_devanagari(form)
            for p in prakriya:
                if p["number"] not in dhAtu_details:
                    dhAtu_details[p["number"]] = (p["dhAtu"], p["verbaccent"],
                                 p["meaning"], p["gana"], p["number"])
                dhAtu_forms[p["number"]][(p["lakara"], p["suffix"])].append(form)
            if i % 1000 == 0:
                logger.info("%d", i)
#            if i == 1000:
#                break

    logger.info("Done. Writing output ...")
    numbers_sorted = sorted(dhAtu_details.keys())
    with codecs.open(out_file, "wb", encoding="utf-8") as out:
        out.write("\t".join(headers) + "\n")
        for n in numbers_sorted:
            out.write(to_tsv_row(dhAtu_details[n], dhAtu_forms[n]))
            
    logger.info("Done")
    logger.info("Took %s", time.time() - start)
