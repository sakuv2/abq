import datetime
from typing import List
from pydantic import create_model

_convert_type = dict(
    INTEGER=int,
    FLOAT=float,
    STRING=str,
    BYTES=bytes,
    TIME=datetime.time,
    DATE=datetime.date,
    DATETIME=datetime.datetime,
    TIMESTAMP=datetime.datetime,
    BOOLEAN=bool,
)


class RowsParser:
    def __init__(self, schema):
        self.schemas = _parse_schema(schema["fields"])
        self.Model = _make_model(self.schemas)

    def parse_rows(self, rows):
        new_rows = _rec(rows, self.schemas)
        ListModel = create_model("", rows=(List[self.Model], ...))
        return ListModel(rows=new_rows).dict()["rows"]


def _parse_schema(fields):
    """
    以下のようなjsonを
    {'fields': [{'name': 'arr',
    'type': 'RECORD',
    'mode': 'REPEATED',
    'fields': [{'name': 'a', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'b', 'type': 'DATE', 'mode': 'NULLABLE'}]},
    {'name': 'st',
    'type': 'RECORD',
    'mode': 'NULLABLE',
    'fields': [{'name': 'ss',
        'type': 'RECORD',
        'mode': 'NULLABLE',
        'fields': [{'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'}]},
        {'name': 'names', 'type': 'STRING', 'mode': 'REPEATED'}]},
    {'name': 'boo', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}]}
    以下のようにする
    {'arr': [{'a': int, 'b': datetime.date}],
    'st': {'ss': {'id': int}, 'names': [str]},
    'boo': bool}
    """
    fs = {}
    for field in fields:
        name = field["name"]
        mode = field["mode"]
        type_ = field["type"]
        if type_ == "RECORD":
            fields = field["fields"]
            cfs = _parse_schema(fields)
        else:
            cfs = _convert_type[type_]
        obj = [cfs] if mode == "REPEATED" else cfs
        fs[name] = obj
    return fs


def _rec(rows, schemas):
    """
    以下のようなjsonを
    [
        {'v': [
            {'v': {'f': [{'v': '1'}, {'v': '2020-01-01'}]}},
            {'v': {'f': [{'v': '2'}, {'v': '2020-01-02'}]}}
        ]},
        {'v': {'f': [
            {'v': {'f': [{'v': '1'}]}},
            {'v': [{'v': 's1'}, {'v': 's2'}]}
        ]}},
        {'v': 'true'}
    ]
    以下のようにする
    {'arr': [{'a': '1', 'b': '2020-01-01'}, {'a': '2', 'b': '2020-01-02'}],
    'st': {'ss': {'id': '1'}, 'names': ['s1', 's2']},
    'boo': 'true'}
    """
    new_rows = []
    for row in rows:
        fs = row["f"]  # list for value
        rs = {}
        for f, schema in zip(fs, schemas.items()):  # required python ^3.7
            v = f["v"]
            name = schema[0]
            c_schemas = schema[1]
            if isinstance(v, list):
                if isinstance(c_schemas[0], dict):
                    rs[name] = [_rec([_row["v"]], c_schemas[0])[0] for _row in v]
                else:
                    rs[name] = [_row["v"] for _row in v]

            elif isinstance(v, dict):
                rs[name] = _rec([v], c_schemas)[0]
            else:
                rs[name] = v
        new_rows.append(rs)
    return new_rows


def _make_model(fs, n="_"):
    """_parse_schemaで得た結果に従い, pydanticのモデルを動的に作成する
    """
    values = {}
    for name, type_ in fs.items():
        if isinstance(type_, list):
            if isinstance(type_[0], dict):
                value = (List[_make_model(type_[0], n + name)], ...)
            else:
                value = (List[type_[0]], ...)
        elif isinstance(type_, dict):
            value = (_make_model(type_, n + name), ...)
        else:
            value = (type_, ...)
        values[name] = value

    model = create_model(n, **values)
    return model
