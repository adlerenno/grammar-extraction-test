import csv, re, os
import datetime
from collections import defaultdict
from os.path import isfile


def parse_filename():
    pass


def get_success_indicator(filename) -> str:
    if os.path.isfile(filename):
        with open(filename, 'r') as f:
            for line in f:
                return line[0]
            return '0'
    else:
        # print(f'indicator "{filename}" is missing. I assume failure.')
        return '0'
        # raise FileNotFoundError(f'File indicators/{filename}.{file_extension}.{approach} not found.')


def add_stats(data, filename):
    if os.path.isfile(f'bench/{filename}.dec.csv'):
        with open(f'bench/{filename}.dec.csv', 'r') as g:
            reader = csv.reader(g, delimiter="\t")
            next(reader)  # Headers line
            bench_data = next(reader)
            # 's', 'h:m:s', 'max_rss', 'max_vms', 'max_uss', 'max_pss', 'io_in', 'io_out', 'mean_load', 'cpu_time'
            data[0] += bench_data[0]
            data[1] = str(sum((datetime.timedelta(*map(int, t.split(':')[::-1])) for t in (data[1], bench_data[1])), datetime.timedelta()))  # adds two h:m:s formats.
            data[2] = max(data[2], bench_data[2])
            data[3] = max(data[3], bench_data[3])
            data[4] = max(data[4], bench_data[4])
            data[5] = max(data[5], bench_data[5])
            data[6] += bench_data[6]
            data[7] += bench_data[7]
            data[8] = max(data[8], bench_data[8])  # Not good, but I won't evaluate it anyway.
            data[9] += bench_data[9]
            return data
    else:
        bench_data = ['NA' for _ in range(10)]
    return bench_data



def get_file_size(filename) -> int:
    if os.path.isfile(filename):
        return os.path.getsize(filename)
    else:
        return -1


def combine_comp(DATA_SETS, out_file):
    with open(out_file, "w") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(['dataset', 'original_file_size', 'compressed_file_size', 's', 'h:m:s', 'max_rss', 'max_vms', 'max_uss', 'max_pss', 'io_in', 'io_out', 'mean_load', 'cpu_time'])
        for data_set in DATA_SETS:
                bench = f'bench/{data_set}.csv'
                file_original_size = get_file_size(f'source/{data_set}')
                file_compressed_size = get_file_size(f'data/{data_set}')
                if isfile(bench):
                    with open(bench, 'r') as g:
                        reader = csv.reader(g, delimiter="\t")
                        next(reader)  # Headers line
                        bench_data = next(reader)
                else:
                    bench_data = ['NA' for _ in range(10)]
                writer.writerow([data_set, str(file_original_size), str(file_compressed_size)] + bench_data)


def combine_query(DATA_SETS, APPROACHES, QUERY_LENGTH, out_file):
    with (open(out_file, "w") as f):
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(
            ['algorithm', 'dataset', 'type', 'query_count', 'successful', 's', 'h:m:s', 'max_rss', 'max_vms', 'max_uss', 'max_pss', 'io_in',
             'io_out', 'mean_load', 'cpu_time'])
        for data_set in DATA_SETS:
            query_count = defaultdict(lambda: 0)
            for k in range(len(QUERY_LENGTH)):
                query_count[k] = sum(1 for _ in open(f'queries/{data_set}.{QUERY_LENGTH[k]}'))

            for approach in APPROACHES:
                for k in range(1, len(QUERY_LENGTH)):
                    bench = f'bench/{data_set}.{QUERY_LENGTH[k]}.{approach}.csv'
                    indicator = get_success_indicator(f'indicators/{data_set}.{QUERY_LENGTH[k]}.{approach}')
                    if isfile(bench):
                        with open(bench, 'r') as g:
                            reader = csv.reader(g, delimiter="\t")
                            next(reader)  # Headers line
                            bench_data = next(reader)
                            if approach == 'dec':
                                bench_data = add_stats(list(bench_data), data_set)
                    else:
                        bench_data = ['NA' for _ in range(10)]
                    writer.writerow([approach, data_set, str(QUERY_LENGTH[k]), str(query_count[k]), indicator] + bench_data)
