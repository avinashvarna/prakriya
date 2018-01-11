# -*- coding: utf-8 -*-
"""
Convert prakriya data to csv format

@author: alvarna
"""

from __future__ import print_function
import os.path
import json
import tarfile
import time
import logging
import codecs

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
            if item in ['gana', 'padadecider_id', 'padadecider_sutra', 'number', 
                        'meaning', 'lakara', 'verb', 'it_status', 'it_sutra', 
                        'purusha', 'vachana', 'upasarga', 'suffix']:
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

fields = ["dhAtu", "verbaccent", "meaning", "gana", "number",
          "lakara", "purusha", "vachana", "suffix", "it_status"]

def to_csv_row(prakriya):
    for p in prakriya:
        yield ','.join((p[x] for x in fields))

if __name__ == '__main__':
    start = time.time()
    logging.basicConfig(level=logging.INFO)
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    forms_file = os.path.join(data_dir, "derivation_v003.tar.gz")
    out_file = os.path.join(data_dir, "dhaval_prakriya_forms.csv")
    logger.info("Reading from tar file of forms and generating csv ...")
    with tarfile.open(forms_file, 'r') as tf, \
            codecs.open(out_file, "wb", encoding="utf-8") as out:
        out.write(",".join(fields) + ",form\n")
        files = tf.getmembers()
        for i, file in enumerate(files):
            if not file.isfile(): 
                continue
            f = tf.extractfile(file)
            prakriya = get_full_data(f)
            form = os.path.splitext(os.path.basename(file.name))[0]
            form = to_devanagari(form)
            for r in to_csv_row(prakriya):
                out.write(r + "," + form + "\n")
            if i % 1000 == 0:
                logger.info("%d", i)
#            if i == 1000:
#                break
            
    logger.info("Done")
    logger.info("Took %s", time.time() - start)
