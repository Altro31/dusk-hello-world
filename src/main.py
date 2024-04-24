import dask.bag as db

from functions.word_counter import word_counter
from utils.pause import pause

if __name__ == "__main__":
    text = db.read_text("src/resources/don_quijote.txt")
    result = word_counter(text.compute())
    print(result)
    pause()
