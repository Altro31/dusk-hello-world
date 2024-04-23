import dask.bag as db
from dask.distributed import Client

if __name__ == '__main__':
    client = Client()

    lista = db.range(3, 2)

    count = lista.accumulate(lambda x, y: x + y)

    total_count = count.fold(lambda acc, curr: acc + curr)

    print(total_count.compute())