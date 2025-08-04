import time
from util import procesar
from processor import Processor
if __name__ == "__main__":
    input_file = 'data/input/1gb_file.txt'
    output_file = 'data/output/1gb_output.txt'
    print("Iniciado procesamiento de "+ input_file)
    Processor.procesar_archivo(input_file, output_file)
    print("Procesamiento finalizado")

    tiempo_inicial2 = time.time()
    lineas = []
    with open(input_file, 'r') as lector:
        while True:
            linea = lector.readline()
            if not linea:
                break
            lineas.append(linea)

    with open(output_file, 'a+') as escritor:
        for linea in lineas:

            escritor.write(procesar(linea) + '\n')

    print(f"Tiempo total con approach mas simple: {time.time() -tiempo_inicial2}")