def flatten(d, parent=''):
    items = {}
    for k, v in d.items():
        novo = f"{parent}_{k}" if parent else k
        if isinstance(v, dict):
            items.update(flatten(v, novo))
        elif isinstance(v, list):
            if not v:
                items[novo] = None
            elif isinstance(v[0], dict):
                for i, item in enumerate(v):
                    items.update(flatten(item, f"{novo}_{i}"))
            else:
                items[novo] = ', '.join(map(str,v))
        else:
            items[novo] = v
    return items

def flatten_num(dados): 
    registros = []
    for item in dados:
        indicador = next(iter(item))
        valor = item[indicador]

        if not isinstance(valor, list):
            registros.append({
                "indicador": indicador,
                "indice": None,
                "valor": valor
            })
        else:
            for indice, elemento in enumerate(valor):
                if not isinstance(elemento, dict):
                    registros.append({
                        "indicador": indicador,
                        "indice": indice,
                        "valor": elemento
                    })
                else:
                    registro = {
                        "indicador": indicador,
                        "indice": indice
                    }
                    registro.update(elemento)
                    registros.append(registro)
    return registros