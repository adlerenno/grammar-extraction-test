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

APPROACHES = [
    'er', # extract than recompress
    'dec' # uncompress, extract than compress
]


DATA_SETS = [
    'sources',
    'pitches',
    'proteins',
    'dna',
    'english',
    'dblp',
]
QUERY_LENGTH = [10**i for i in range(3,8)]
OMITTED_COMBINATIONS = []

FILES = [f'indicators/{file}.{length}.{approach}'
         for approach in APPROACHES
         for length in QUERY_LENGTH
         for file in DATA_SETS
         if not (approach, length, file) in OMITTED_COMBINATIONS
         ]

for path in [BENCHMARK, SOURCE, INPUT, DATA, OUTPUT, QUERIES, INDICATORS, RESULT]:
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

rule grammar_extract_decompress_extract_compress:
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

rule download_large_scale_source_datasts:
    output:
        out_files = ['']
    shell:
        """./scripts/download_dataset.py -s 200GiB -l all -o ./source
        ./scripts/decompress_dataset.py --dataset <abs_path_archive> -o ./source"""

rule install_java:
    shell:
        """sudo apt install openjdk-21-jdk -y
        sudo apt install gradle -y
        ./gradlew build
        """