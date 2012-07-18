#!/usr/bin/env python2.7

import sys, os, hashlib, itertools
from datetime import datetime
import re, gzip

DELIMITER = os.getenv('DELIMITER', '\xfe')
INPUT_FIELDS = ['Time', 'Elapsed', 'Client', 'Code/Status', 'Bytes', 'Method',
                'Url', 'rfc931', 'PeerStatus/PeerHost', 'Content-Type']

# set constants for input fields
for idx, field in enumerate(INPUT_FIELDS):
  field = re.sub('[^a-zA-Z0-9_]', '_', field, 10)
  globals()['in_' + field] = idx

OUTPUT_FIELDS = ['site_id', 'date_id', 'time_id', 'timestamp_full', 'elapsed',
                 'client', 'code', 'status_id', 'bytes', 'method', 'url',
                 'domain', 'peerstatus', 'peerhost', 'content']


# aprox number of lines for 50 MB of output data assuming a line is 128 bytes long
LINES_50mb = 50 * 1024 * 1024 / 128 * 5 # compression ratio 1:5

def field_parse(stream):
  "make a generator that returns a list of fields per input line"
  return (filter(None, l.rstrip().split(' ')) for l in stream)

# a function to hash strings using md5
UserHash = lambda x: hashlib.md5(x).hexdigest()

# a regex to extract parts of a url
URL_RE = re.compile('^((?P<proto>[^:]+)://)?((?P<user>[^:@]+)(:(?P<pass>[^@]+))?@)?(?P<domain>[^:/]+)(:(?P<port>[0-9]+))?(?P<request>/.*)?$')

def format_fields(stream):
  site_id = '1'
  for line in stream:
    # time related fields
    timestamp_full = float(line[in_Time])
    dt = datetime.fromtimestamp(timestamp_full)
    date_id = dt.strftime('%Y%m%d')
    time_id = str(dt.hour * 60 + dt.minute)
    timestamp_full = '{:.0f}'.format(timestamp_full * 1e6)
    # other fields
    code, sep, status_id = line[in_Code_Status].partition('/')
    domain = URL_RE.search(line[in_Url])
    if domain:
      domain = domain.group('domain')
    else:
      domain = ''
    # output the line
    peerstatus, sep, peerhost = line[in_PeerStatus_PeerHost].partition('/')
    yield [site_id, date_id, time_id, timestamp_full, line[in_Elapsed],
           UserHash(line[in_Client]), code, status_id, line[in_Bytes],
           line[in_Method], line[in_Url], domain, peerstatus, peerhost,
           line[in_Content_Type]]


def chunker(stream, chunk_lines):
  for chunk in itertools.count():
    try:
      beacon = next(stream)
    except StopIteration:
      break
    yield itertools.chain((beacon,), itertools.islice(stream, chunk_lines))


def output_delim(stream, chunk_template, chunk_num):
  with open('{:s}_{:02d}.log'.format(chunk_template, chunk_num), 'wb') as out:
    out.write(DELIMITER.join(OUTPUT_FIELDS) + "\n")
    for line in stream:
      out.write(DELIMITER.join(line) + "\n")


# write gzip'ed output in another process
def gziped_output_delim(stream, chunk_tpl, chunk_num):
  import multiprocessing as mp
  read_pipe, write_pipe = mp.Pipe(False)

  def gziped_write():
    write_pipe.close()
    filename = '{:s}_{:02d}.log.gz'.format(chunk_tpl, chunk_num)
    with gzip.open(filename, 'wb') as out:
      out.write(DELIMITER.join(OUTPUT_FIELDS) + "\n")
      try:
        while True:
          out.write(DELIMITER.join(read_pipe.recv()) + "\n")
      except EOFError:
        read_pipe.close()

  writter = mp.Process(target=gziped_write)
  writter.start()
  read_pipe.close()
  for line in stream: write_pipe.send(line)
  write_pipe.close()
  writter.join()


if __name__ == '__main__':
  if len(sys.argv) < 2:
    print("usage: logformatter.py <output-prefix>")
    sys.exit(1)

  parsed = field_parse(sys.stdin)
  formatted = format_fields(parsed)
  chunked = chunker(formatted, LINES_50mb)

  for i, chunk in enumerate(chunked):
    #output_delim(chunk, sys.argv[1], i)
    gziped_output_delim(chunk, sys.argv[1], i)


# vim: set sw=2 sts=2 : #
