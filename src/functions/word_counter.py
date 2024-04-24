import dask.bag as db
from distributed import LocalCluster

from src.utils.words_extractor import words_extractor


def word_counter(text: str | list[str]):
    if type(text) == list:
        text = ''.join(text)

    print(f"🚀 Dashboard at http://127.0.0.1:8787/status")

    cluster = LocalCluster()
    client = cluster.get_client()

    words_list = words_extractor(text)

    partitioned_text = db.from_sequence(words_list, npartitions=4)

    task = partitioned_text.map_partitions(_word_map)

    result = task.fold(_increment_count)

    return result.compute()


def _increment_count(mapper: dict, words: str | dict):
    if type(words) != dict:
        words = {words: 1}

    for word in words.keys():
        if mapper.get(word) is None: mapper[word] = 0
        mapper[word] += words[word]

    return mapper


def _word_map(partition: list[str]):
    mapper = {}
    for word in partition:
        _increment_count(mapper, word)

    return [mapper]
