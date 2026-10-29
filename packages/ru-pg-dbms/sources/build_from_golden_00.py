#!/usr/bin/env python3
"""Пересборка каталога и golden из сравнительной таблицы golden_00.xlsx."""
from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import yaml
from openpyxl import load_workbook

SOURCE = Path('/mnt/c/Work/htap/golden_00.xlsx')
PACKAGE = Path(__file__).resolve().parent.parent
SHEET = 'Сравнительная таблица'
PRODUCTS = ['digital_q_database', 'pangolin', 'postgrespro', 'tantor', 'jatoba']
METADATA = {1, 2, 3, 14, 15, 82, 137}
# Регрессионная страта: весь раздел «Масштабируемость и надежность» — параметры
# 47–77, 31 параметр × 5 продуктов = 155 ячеек. Выбор фиксирован автором
# пакета, а ожидаемые статусы и provenance берутся только из XLSX.
FAST_PARAMS = set(range(47, 78))
TRANS = str.maketrans({'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'h','ц':'c','ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya'})
HEADERS = ['param_id','title','module','group','importance','param_type','rule','absence_rule','role_binding','concept_terms','search_langs']
MAP_HEADERS = ['product_id','excel_number','section','excel_title','raw_value','normalized_status','param_id','source_url','source_comment','hyperlink_target']

def slug(text: str) -> str:
    text = unicodedata.normalize('NFKD', text.lower()).translate(TRANS)
    text = ''.join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r'[^a-z0-9]+', '_', text).strip('_')[:52].rstrip('_')
    return text or 'parameter'

def main() -> None:
    wb = load_workbook(SOURCE, data_only=False)
    ws = wb[SHEET]
    params=[]; mapping=[]; golden=[]; section=''; seen={}
    for row in range(2, ws.max_row + 1):
        number=ws.cell(row,1).value
        if not isinstance(number,(int,float)):
            if ws.cell(row,2).value is None and number: section=str(number)
            continue
        number=int(number); title=str(ws.cell(row,2).value or '').strip()
        base=slug(title); seen[base]=seen.get(base,0)+1
        pid=f'xlsx_{number:03d}_{base}'
        module = 'backup' if 59 <= number <= 77 else ('management' if section in {'Удобство сопровождения','Возможности графического интерфейса'} else 'core')
        group=slug(section)
        role='backup_recovery' if module=='backup' else ('management_platform' if module=='management' else ('migration' if section=='Миграция' else 'core_engine'))
        terms={'ru':[title]+[t.lower() for t in re.findall(r'[\w]+',title,re.UNICODE) if len(t)>2], 'en':[t.lower() for t in re.findall(r'[A-Za-z0-9]+',title) if len(t)>1]}
        ismeta=number in METADATA
        params.append([pid,title,module,group,'Вж' if ismeta else 'Баз','fact',
          'Засчитывается наличие явно указанного значения в исходной comparative table; само значение сохраняется в provenance.' if ismeta else 'Засчитывается только явно подтверждённое значение «ДА»; «ЧАСТИЧНО» остаётся ⚠️, «НЕТ» — ❌.',
          'explicit',role,json.dumps(terms,ensure_ascii=False,separators=(',',':')),'ru|en'])
        for col, product in enumerate(PRODUCTS,3):
            cell=ws.cell(row,col); raw='' if cell.value is None else ' '.join(str(cell.value).split())
            status=('✅' if raw else '') if ismeta else {'ДА':'✅','ЧАСТИЧНО':'⚠️','НЕТ':'❌'}.get(raw,'')
            klass='metadata' if ismeta else {'✅':'class_ok','⚠️':'class_partial','❌':'class_absent'}.get(status,'metadata')
            short_raw=' '.join(raw[:120].split())
            comment = cell.comment.text if cell.comment else ''
            hyperlink = cell.hyperlink.target if cell.hyperlink and cell.hyperlink.target else ''
            urls = re.findall(r'https?://[^\s)]+', comment)
            source_url = hyperlink or (urls[0] if urls else '')
            tier = 'fast' if number in FAST_PARAMS else 'full'
            golden.append([product,pid,status,'documentation',klass,tier,f'source=golden_00.xlsx;sheet={SHEET};excel_number={number};sheet_row={row};raw={short_raw}'])
            mapping.append([product,number,section,title,raw,status,pid,source_url,comment,hyperlink])
    def write(name, headers, rows):
        with (PACKAGE/name).open('w',encoding='utf-8',newline='') as f:
            w=csv.writer(f,lineterminator='\n'); w.writerow(headers); w.writerows(rows)
    write('catalog.csv',HEADERS,params)
    write('golden.csv',['product_id','param_id','expected_status','expected_evidence_kind','case_class','tier','note'],golden)
    write('sources/golden_00_cells.csv',MAP_HEADERS,mapping)
    (PACKAGE/'baselines.csv').write_text('baseline_id,param_id,min_version,inheritance_mode,note\n',encoding='utf-8')
    (PACKAGE/'catalog.lock.csv').write_text('param_id,source_file,source_row,title,generated_at\n',encoding='utf-8')
    sha=hashlib.sha256(SOURCE.read_bytes()).hexdigest()
    provenance={'case_id':'ru-pg-dbms','generated_at':'2026-10-28T00:00:00Z','generator':'packages/ru-pg-dbms/sources/build_from_golden_00.py','snapshot_date':'2026-10-28','sources':[{'path':str(SOURCE),'role':'сравнительная таблица и исходные статусы 181 параметра','sha256':sha,'sheet':SHEET}], 'superseded_sources':[{'path':'/mnt/c/Work/gtm/psq_params_091_demo.xlsx','role':'прежний технический каталог 119 параметров','sheet':'final'}, {'path':'/mnt/c/Work/gtm/golden_091.xlsx','role':'прежний неполный golden-набор','sheet':'data'}]}
    (PACKAGE/'sources/provenance.yaml').write_text(yaml.safe_dump(provenance,allow_unicode=True,sort_keys=False),encoding='utf-8')

if __name__=='__main__': main()
