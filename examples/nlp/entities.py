import argparse

from seldonite import collect, run, sources, nlp

def main(args):

    csv_source = sources.CSV(args.input)
    collector = collect.Collector(csv_source)

    nl_processor = nlp.NLP(collector) \
        .get_entities()

    runner = run.Runner(nl_processor) 
    df = runner.to_pandas()

    df.to_csv(args.out)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    parser.add_argument('-o', '--out')
    args = parser.parse_args()

    main(args)