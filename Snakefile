import os

# sudo snakemake --rerun-triggers mtime --cores 1
# sudo snakemake --touch --cores 1

MAX_MAIN_MEMORY = 128
NUMBER_OF_PROCESSORS = 32

DIR = "./"
SOURCE = './source/'
INPUT = './input/'
DATA = './data/'
OUTPUT = './compressed/'
QUERIES = './queries/'
INDICATORS = './indicators/'
BENCHMARK = './bench/'
RESULT = './results/'
DECOMB = './decomb/'  # Directory for help files for dec method.

APPROACHES = [
    'er', # extract than recompress
    'dec', # uncompress, extract than compress
    'e' # only extract
]


DATA_SETS = [
    'sources',
    'pitches',
    'proteins',
    'dna',
    'english',
    'dblp',
    'github50',
    'wikidump'

]
QUERY_LENGTH = [10**i for i in range(3,8)]
OMITTED_COMBINATIONS = []

FILES = [f'indicators/{file}.{length}.{approach}'
         for approach in APPROACHES
         for length in QUERY_LENGTH
         for file in DATA_SETS
         if not (approach, length, file) in OMITTED_COMBINATIONS
         ]

for path in [BENCHMARK, SOURCE, INPUT, DATA, OUTPUT, QUERIES, INDICATORS, RESULT, DECOMB]:
    os.makedirs(path, exist_ok=True)


rule target:
    input:
        comp_bench = 'results/comp_benchmark.csv',
        query_bench = 'results/query_benchmark.csv'

rule get_file_stats:
    input:
        set = FILES
    output:
        bench = 'results/comp_benchmark.csv'
    run:
        from scripts.collect_benchmark import combine_comp
        combine_comp(DATA_SETS, output.bench)

rule get_query_results:
    input:
        set = FILES
    output:
        bench = 'results/query_benchmark.csv'
    run:
        from scripts.collect_benchmark import combine_query
        combine_query(DATA_SETS, APPROACHES, QUERY_LENGTH, output.bench)


rule clean:
    shell:
        """
        rm -rf ./source
        rm -rf ./input
        rm -rf ./data
        rm -rf ./compressed
        rm -rf ./queries
        rm -rf ./indicators
        rm -rf ./bench
        rm -rf ./results
        rm -rf ./decomb
        """

rule grammar_extract_sakai_et_al:
    input:
        source = 'data/{filename}',
        queries = 'queries/{filename}.{length}'
    output:
        indicator = 'indicators/{filename}.{length}.er'
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}.{length}.er.csv'
    shell:
        """all_success=true
        while IFS=' ' read -r num1 num2; do
            [[ -z "$num1" || -z "$num2" ]] && continue  # skip empty lines
            echo "Query $num1 to $num2"
            java -jar grammarextractor_current.jar -r -InputFile {input.source} -OutputFile temp.dumb -from "$num1" -to "$num2" -passes 0
            wc -c < comp.dumb >> bench/{wildcards.filename}.{wildcards.length}.er.filesizes.csv
            if [[ $? -ne 0 ]]; then
                all_success=false
                echo "extract-recmpress failed on {input.source} for $num1 $num2"
            fi
        done < {input.queries}
        if $all_success; then
            echo 1 > {output.indicator}
        else
            echo 0 > {output.indicator}
        fi
        """

rule grammar_extract_extract_only:
    input:
        source = 'data/{filename}',
        queries = 'queries/{filename}.{length}'
    output:
        indicator = 'indicators/{filename}.{length}.e'
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}.{length}.e.csv'
    shell:
        """all_success=true
        while IFS=' ' read -r num1 num2; do
            [[ -z "$num1" || -z "$num2" ]] && continue  # skip empty lines
            echo "Query $num1 to $num2"
            java -jar grammarextractor_current.jar -e -InputFile {input.source} -OutputFile temp.dumb -from "$num1" -to "$num2" -passes 0
            wc -c < comp.dumb >> bench/{wildcards.filename}.{wildcards.length}.e.filesizes.csv
            if [[ $? -ne 0 ]]; then
                all_success=false
                echo "extract failed on {input.source} for $num1 $num2"
            fi
        done < {input.queries}
        if $all_success; then
            echo 1 > {output.indicator}
        else
            echo 0 > {output.indicator}
        fi
        """

rule grammar_extract_decompress_extract_compress_1:
    input:
        source = 'data/{filename}'
    output:
        decomp = 'decomb/{filename}'
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}.dec.csv'
    shell:
        """
        java -jar grammarextractor_current.jar -d -InputFile {input.source} -OutputFile {output.decomp} -from "$num1" -to "$num2"
        """

rule grammar_extract_decompress_extract_compress_2:
    input:
        source = 'data/{filename}',
        queries = 'queries/{filename}.{length}'
    output:
        indicator = 'indicators/{filename}.{length}.dec'
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}.{length}.dec.csv'
    shell:
        """all_success=true
        while IFS=' ' read -r num1 num2; do
            [[ -z "$num1" || -z "$num2" ]] && continue  # skip empty lines
            echo "Query $num1 to $num2"
            java -jar grammarextractor_current.jar -e -InputFile {input.source} -OutputFile temp.dumb -from "$num1" -to "$num2"
            java -jar grammarextractor_current.jar -c -InputFile temp.dumb -from "$num1" -to "$num2"
            wc -c < temp.dumb.rp >> bench/{wildcards.filename}.{wildcards.length}.dec.filesizes.csv
            if [[ $? -ne 0 ]]; then
                all_success=false
                echo "extract-recmpress failed on {input.source} for $num1 $num2"
            fi
        done < {input.queries}
        if $all_success; then
            echo 1 > {output.indicator}
        else
            echo 0 > {output.indicator}
        fi
        """

rule reformat_to_human_readable:
    input:
        source = 'input/{filename}.rp',
    output:
        compressed_file = 'data/{filename}'
    shell:
        """./decoder_mac {input.source} {output.compressed_file}"""

rule grammar_compress:
    input:
        source = 'source/{filename}'
    output:
        compressed_file = 'input/{filename}.rp'
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}.csv'
    shell:
        """java -jar grammarextractor_current.jar -c -InputFile {input.source}
        mv {input.source}.rp {output.compressed_file}"""

rule generate_queries:
    input:
        test_file = 'source/{file}'
    output:
        queries_file = 'queries/{file}.{length}'
    run:
        from scripts.generate_queries import generate_queries
        generate_queries(input.test_file, output.queries_file, int(wildcards.length), 100)


rule download_pizza_and_chili_sources:
    output:
        out_file = 'source/sources'
    shell:
        """
        cd source
        URL="https://pizzachili.dcc.uchile.cl/texts/code/sources.gz"
        FILENAME = "sources.gz"
        RESULT = "sources"
        if [ ! -f "$FILENAME" ]; then
            curl -O "$URL"
        else
            echo "$FILENAME already exists. Skipping download."
        fi
        if [ ! -f "$RESULT" ]; then
            gzip -dk "$FILENAME"
        else
            echo "$RESULT already exists. Skipping decompression."
        fi
        """

rule download_wikidump:
    output:
        out_file = 'source/'
    shell:
        """
        cd source
        wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
        bzip2 -dk enwiki-latest-pages-articles.xml.bz2
        mv enwiki-latest-pages-articles.xml wikidump
        """

rule download_50GB_github:
    output:
        out_file = 'source/github50'
    shell:
        """
        git clone https://github.com/acubeLab/PPC_utils4BigData
        cd PPC_utils4BigData
        ./download_dataset.py -s 50GiB -o ./tmp
        abs_path="$(realpath ./tmp/50GiB_github_filename_sort_50GiB.tar.zstd_22)"
        ./decompress_dataset.py "$abs_path" --dataset -o tmp/blobs
        find ./tmp/blobs -type f -print0 | sort -z | xargs -0 cat -- >> ../source/github50
        """
# /PPC_utils4BigData$ ./download_dataset.py -s 50GiB -o ./tmp
# Output directory: /data/grammar-extraction-test/PPC_utils4BigData/tmp
# Downloading from the https://pages.di.unipi.it/boffa/swh_endpoint
# --2025-12-03 16:09:17--  https://pages.di.unipi.it/boffa/swh_endpoint/50GiB_github/50GiB_github_filename_sort_50GiB.tar.zstd_22
# Resolving pages.di.unipi.it (pages.di.unipi.it)... 131.114.2.105
# Connecting to pages.di.unipi.it (pages.di.unipi.it)|131.114.2.105|:443... connected.
# HTTP request sent, awaiting response... 200 OK
# Length: 5453429452 (5.1G) [application/x-tar]
# Saving to: ‘50GiB_github_filename_sort_50GiB.tar.zstd_22’
#
# 50GiB_github_filena 100%[===================>]   5.08G  7.26MB/s    in 12m 14s
#
# 2025-12-03 16:21:31 (7.09 MB/s) - ‘50GiB_github_filename_sort_50GiB.tar.zstd_22’ saved [5453429452/5453429452]
#
# Fatal: Cannot download the 50GiB archives
#  'tuple' object has no attribute 'decode'

# ./decompress_dataset.py ./tmp/repos_all_compressed.tar.zstd_22 --dataset -o tmp/repos --force


rule download_pizza_and_chili_pitches:
    output:
        out_file='source/pitches'
    shell:
        """
        cd source
        URL="https://pizzachili.dcc.uchile.cl/texts/music/pitches.gz"
        FILENAME="pitches.gz"
        RESULT="pitches"
        if [ ! -f "$FILENAME" ]; then
            curl -O "$URL"
        else
            echo "$FILENAME already exists. Skipping download."
        fi
        if [ ! -f "$RESULT" ]; then
            gzip -dk "$FILENAME"
        else
            echo "$RESULT already exists. Skipping decompression."
        fi
        """

rule download_pizza_and_chili_proteins:
    output:
        out_file='source/proteins'
    shell:
        """
        cd source
        URL="https://pizzachili.dcc.uchile.cl/texts/protein/proteins.gz"
        FILENAME="proteins.gz"
        RESULT="proteins"
        if [ ! -f "$FILENAME" ]; then
            curl -O "$URL"
        else
            echo "$FILENAME already exists. Skipping download."
        fi
        if [ ! -f "$RESULT" ]; then
            gzip -dk "$FILENAME"
        else
            echo "$RESULT already exists. Skipping decompression."
        fi
        """

rule download_pizza_and_chili_dna:
    output:
        out_file='source/dna'
    shell:
        """
        cd source
        URL="https://pizzachili.dcc.uchile.cl/texts/dna/dna.gz"
        FILENAME="dna.gz"
        RESULT="dna"
        if [ ! -f "$FILENAME" ]; then
            curl -O "$URL"
        else
            echo "$FILENAME already exists. Skipping download."
        fi
        if [ ! -f "$RESULT" ]; then
            gzip -dk "$FILENAME"
        else
            echo "$RESULT already exists. Skipping decompression."
        fi
        """

rule download_pizza_and_chili_english:
    output:
        out_file='source/english'
    shell:
        """
        cd source
        URL="https://pizzachili.dcc.uchile.cl/texts/nlang/english.gz"
        FILENAME="english.gz"
        RESULT="english"
        if [ ! -f "$FILENAME" ]; then
            curl -O "$URL"
        else
            echo "$FILENAME already exists. Skipping download."
        fi
        if [ ! -f "$RESULT" ]; then
            gzip -dk "$FILENAME"
        else
            echo "$RESULT already exists. Skipping decompression."
        fi
        """

rule download_pizza_and_chili_dblp:
    output:
        out_file='source/dblp'
    shell:
        """
        cd source
        URL="https://pizzachili.dcc.uchile.cl/texts/xml/dblp.xml.gz"
        FILENAME="dblp.xml.gz"
        RESULT="dblp.xml"
        if [ ! -f "$FILENAME" ]; then
            curl -O "$URL"
        else
            echo "$FILENAME already exists. Skipping download."
        fi
        if [ ! -f "$RESULT" ]; then
            gzip -dk "$FILENAME"
        else
            echo "$RESULT already exists. Skipping decompression."
        fi
        mv $RESULT dblp 
        """

rule install_java:
    shell:
        """sudo apt install openjdk-21-jdk -y
        sudo apt install gradle -y
        ./gradlew build
        """