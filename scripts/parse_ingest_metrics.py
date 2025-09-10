#!/usr/bin/env python3
import re
import sys
from statistics import mean

pattern = re.compile(
    r"ingest_metrics table=(?P<table>\S+) mode=(?P<mode>\S+) op=(?P<op>\S+) "
    r"(?:(?:parse_ms=(?P<parse_ms>\d+))|(?:parse_us=(?P<parse_us>\d+))) "
    r"(?:(?:build_ms=(?P<build_ms>\d+))|(?:build_us=(?P<build_us>\d+))) "
    r"(?:(?:send_ms=(?P<send_ms>\d+))|(?:send_us=(?P<send_us>\d+))) "
    r"(?:(?:wait_lsn_ms=(?P<wait_ms>\d+))|(?:wait_lsn_us=(?P<wait_us>\d+))) "
    r"(?:(?:total_ms=(?P<total_ms>\d+))|(?:total_us=(?P<total_us>\d+)))"
)

def parse_file(path: str):
    rows = []
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            m = pattern.search(line)
            if not m:
                continue
            d = m.groupdict()
            # prefer microseconds if present, convert to milliseconds for reporting
            def pick_ms(ms_key, us_key):
                if d.get(us_key):
                    return int(d[us_key]) / 1000.0
                return float(d[ms_key]) if d.get(ms_key) else 0.0

            rows.append({
                'table': d['table'],
                'mode': d['mode'],
                'op': d['op'],
                'parse': pick_ms('parse_ms', 'parse_us'),
                'build': pick_ms('build_ms', 'build_us'),
                'send': pick_ms('send_ms', 'send_us'),
                'wait': pick_ms('wait_ms', 'wait_us'),
                'total': pick_ms('total_ms', 'total_us'),
            })
    return rows

def summarize(rows):
    if not rows:
        print('No ingest_metrics lines found')
        return
    by_key = {}
    for r in rows:
        key = (r['table'], r['mode'], r['op'])
        by_key.setdefault(key, []).append(r)
    print('Averages by (table, mode, op):')
    print('table\tmode\top\tcount\tparse_ms\tbuild_ms\tsend_ms\twait_lsn_ms\ttotal_ms')
    for key, items in sorted(by_key.items()):
        c = len(items)
        avg_parse = mean(i['parse'] for i in items)
        avg_build = mean(i['build'] for i in items)
        avg_send = mean(i['send'] for i in items)
        avg_wait = mean(i['wait'] for i in items)
        avg_total = mean(i['total'] for i in items)
        print(f"{key[0]}\t{key[1]}\t{key[2]}\t{c}\t{avg_parse:.1f}\t{avg_build:.1f}\t{avg_send:.1f}\t{avg_wait:.1f}\t{avg_total:.1f}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: parse_ingest_metrics.py <log_file>')
        sys.exit(1)
    rows = parse_file(sys.argv[1])
    summarize(rows)


