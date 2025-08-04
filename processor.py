import multiprocessing, threading, os, time, math
from util import procesar

class Processor:
    TAMANO_COLA = 100
    TAMANO_BATCH = 1000
    num_cpus = os.cpu_count()
    HILOS_PROCESADORES = max(1, num_cpus)

    finalizar= "##FIN##"

    @staticmethod
    def producer_lectura(input_cola, input_archivo, num_procesadores, tamano_batch):
        with open(input_archivo, 'r') as lector:
            bacth_actual = []
            for linea in lector:
                bacth_actual.append(linea)
                if len(bacth_actual) >= tamano_batch:
                    input_cola.put(bacth_actual)
                    bacth_actual = []
            if bacth_actual:
                input_cola.put(bacth_actual)
            
        for i in range(num_procesadores):
            input_cola.put(Processor.finalizar)

    @staticmethod
    def consumer_procesamiento(input_cola, output_cola):
        while True:
            batch_de_lineas = input_cola.get()
            if batch_de_lineas == Processor.finalizar:
                output_cola.put(Processor.finalizar)
                break

            batch_procesado =[]
            for linea in batch_de_lineas:
                linea_procesada = procesar(linea)
                batch_procesado.append(linea_procesada)
            output_cola.put(batch_procesado)

    @staticmethod
    def consumer_escritura(output_cola, output_file, num_procesadores):
        procesadores_finalizados = 0
        with open(output_file, 'w') as escritor:
            while True:
                batch_procesado = output_cola.get()
                if batch_procesado == Processor.finalizar:
                    procesadores_finalizados +=1
                    if procesadores_finalizados == num_procesadores:
                        break
                    continue

                for linea in batch_procesado:
                    escritor.write(linea + '\n')
    
    @staticmethod
    def procesar_archivo(input_path: str, output_path: str):
        input_cola = multiprocessing.Queue(Processor.TAMANO_COLA)
        output_cola = multiprocessing.Queue(Processor.TAMANO_COLA)

        tiempo_inicio = time.time()

        trabajadores = []

        hilo_producer = threading.Thread(target=Processor.producer_lectura, 
                                         args=(input_cola, input_path, Processor.HILOS_PROCESADORES, Processor.TAMANO_BATCH )
                                         , name="PRODUCER"
                                         )
        trabajadores.append(hilo_producer)

        for i in range(Processor.HILOS_PROCESADORES):
            hilo_procesador = multiprocessing.Process(target=Processor.consumer_procesamiento, args=(input_cola, output_cola) , name=f"Procesador {i}")
            trabajadores.append(hilo_procesador)
        

        hilo_escritor = threading.Thread(target=Processor.consumer_escritura, args=(output_cola, output_path, Processor.HILOS_PROCESADORES))
        trabajadores.append(hilo_escritor)

        for trabajador in trabajadores:
            trabajador.start()


        for trabajador in trabajadores:
            trabajador.join()

        time_finalizado = time.time()

        print(f"Archivo procesado en {time_finalizado - tiempo_inicio}")
                
