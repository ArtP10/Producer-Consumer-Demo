import math


def procesar(linea: str) -> str:
    numero = 0.0
    for char in linea:
        numero+= math.sin(ord(char))
    return f"{numero}"