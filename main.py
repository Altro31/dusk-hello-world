import dask.bag as db
from dask.distributed import Client

if __name__ == '__main__':
    # Crea el nodo máster y los n Workers (n=Cantidad de núcleos de la CPU)
    client = Client()

    # Crea una lista de números del 0 al 7 y lo particionqa en 4 partes iguales
    # La lista se guarda en un DaskObject de tipo Bag
    # Esto pasa pq se usa db (dask.bag) para crear la lista
    lista = db.range(7, 4)
    
    # Se define la función que cada Worker va a aplicar al conjunto de datos que le llegue
    # En este caso es una función acumuladora, que va iterando valor por valor y va acumulando un valor. Devuelve el valor acumulado.
    count = lista.accumulate(lambda acc, curr: acc + curr)
    
    # Se define la estrategia para recoger los resultados de los Workers
    # La función fold es muy similar a acumulate
    total_count = count.fold(lambda acc, curr: acc + curr)

    # Para acceder a los datos que contiene un DaskObject se debe usar el método compute()
    print(total_count.compute())
