import random
import os
from math import floor


def get_file_length(file_path):
    char_count = 0
    with open(file_path, 'r') as file:
        for line in file:
            char_count += len(line)
    return char_count

def generate_queries(input_file, output_path, query_length, num_queries):
    print('Generating queries for', input_file, 'to', output_path)
    file_length = get_file_length(input_file)
    queries = random.sample(range(file_length-query_length), num_queries)  # - query_length to avoid runout of string.
    print('Generated queries:', queries[1:4])
    # Write to output file
    with open(output_path, 'w') as out:
        for query in queries:
            out.write(f'{query} {query+query_length}\n')

    # print(f"Generated {len(query)} matching queries)