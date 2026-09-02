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