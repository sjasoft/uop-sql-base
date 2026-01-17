from sjasoft.utils.dicts import first_kv
from functools import reduce
from collections import defaultdict
import json


python_sql = dict(
    str="Varchar",
    int="Integer",
    float="Float",
    bool="Boolean",
    json="JSONB",
    email="Varchar[255]",
    phone="Varchar[50]",
    long="Double",
    uuid="Varchar[30]",
    string="Varchar[255]",
    text="Text",
    epoch="Float",
    date="Float",
    datetime="Float",
)


def attribute_string(typed_attributes):
    attr_strings = []

    def type_string(type):
        if type == "string":
            return f"VARCHAR(255)"
        elif type == "int":
            return f"INTEGER"
        elif type == "float":
            return f"FLOAT"
        elif type == "bool":
            return f"BOOLEAN"
        elif type == "list":
            return f"VARCHAR(255)"
        elif type == "dict":
            return f"VARCHAR(255)"
        else:
            return f"VARCHAR(255)"

    for attr, type in typed_attributes.items():
        decl = f"{attr} {type_string(type)}"
        if attr == "id":
            decl = f"{decl}PRIMARY KEY"
        attr_strings.append(decl)
    attrs = ", ".join(attr_strings)
    return f"({attrs})"


infix_operators = {
    "$and": "AND",
    "$or": "OR",
    "$gt": ">",
    "$gte": ">=",
    "$lt": "<",
    "$lte": "<=",
    "$eq": "=",
    "$ne": "!=",
    "like": "LIKE",
    "not like": "NOT LIKE",
    "in": "IN",
    "not in": "NOT IN",
}



class Table:
    def __init__(self, table_name, uop_types, supports_json=False):
        self.name = table_name
        self._typed_attributes = {k: python_sql[v] for k, v in uop_types.items()}
        self._uop_types = uop_types
        self._supports_json = supports_json
        self._modify_json_support()

    def _modify_json_support(self):
        json_type = "JSONB" if self._supports_json else "TEXT"
        for attr, uop_type in self._uop_types.items():
            if uop_type == "json":
                self._typed_attributes[attr] = json_type
                
                
    def json_serialize(self, args):
        if not self._supports_json:
            res = {}
            for k,v in args.items():
                if self._uop_types.get(k) == "json":
                    res[k] = json.dumps(v)
                else:
                    res[k] = v
            return res
        return args

    def json_deserialize(self, args):
        if not self._supports_json:
            for k,v in args.items():
                if self._uop_types.get(k) == "json":
                    args[k] = json.loads(v)
        return args

    def named_parameter(self, name):
        return f"%({name})s"

    def modify_criteria(self, criteria):
        prop_count = defaultdict(int)

        def get_prop_name(prop):
            prop_count[prop] += 1
            return f"{prop}_{prop_count[prop]}"

        def compound_binary(compound_key, *args):
            parts = []
            vals = {}
            for clause, val_map in args:
                parts.append(clause)
                vals.update(val_map)

            clause = "(" + f" {compound_key} ".join(parts) + ")"
            return clause, vals

        def and_clause(*args):
            return compound_binary("AND", *args)

        def or_clause(*args):
            return compound_binary("OR", *args)

        def internal_modify_criteria(criteria):
            vals = {}

            if not criteria:
                return "", vals
            keys = list(criteria.keys())
            if len(keys) > 1:
                parts = [internal_modify_criteria({k: criteria[k]}) for k in keys]
                return and_clause(*parts)
            key = keys[0] if keys else None
            if key in ("$and", "$or"):
                rest = [internal_modify_criteria(c) for c in criteria[key]]
                if key == "$and":
                    return and_clause(*rest)
                elif key == "$or":
                    return or_clause(*rest)
            elif key in infix_operators:
                prop, val = first_kv(criteria[key])
                val_key = get_prop_name(prop)
                operator = infix_operators[key]
                return f"{prop} {operator} {self.named_parameter(val_key)}", {val_key: val}
            elif key == "endswith":
                prop, val = first_kv(criteria[key])
                val_key = get_prop_name(prop)
                val_map = {val_key: f"%{val}"}
                return f"{prop} LIKE {self.named_parameter(val_key)}", val_map

        return internal_modify_criteria(criteria)


    def table_creation_string(self):
        return f"CREATE TABLE {self.name} {attribute_string(self._typed_attributes)}"

    def select_string(self, criteria=None, only_cols=None, order_by=None, limit=None):
        cols = "*" if only_cols is None else ", ".join(only_cols)
        clause, vals = self.modify_criteria(criteria)
        res = f"SELECT {cols} FROM {self.name}"
        if clause:
            res = f"{res} WHERE {clause}"
        return res, vals

    def count_string(self, criteria=None):
        clause, vals = self.modify_criteria(criteria)
        res = f"SELECT COUNT(*) FROM {self.name}"
        if clause:
            res = f"{res} WHERE {clause}"
        return res, vals
    
    def _add_where(self, base, clause):
        return f"{base} WHERE {clause}" if clause else base

    def insert_string(self):
        cols = ", ".join([f"`{k}`" for k in self._typed_attributes.keys()])
        vals = ", ".join([self.named_parameter(k) for k in self._typed_attributes.keys()])
        return f"INSERT INTO {self.name} ({cols}) VALUES ({vals})"

    def mod_vals(self, mods):
        return ", ".join([f"{k} = {self.named_parameter(k)}" for k in mods.keys()])

    def update_string(self, criteria, mods):
        clause, vals = self.modify_criteria(criteria)
        vals.update(mods)
        res = f"UPDATE {self.name} SET {self.mod_vals(mods)}"
        return self._add_where(res, clause), vals     


    def delete_string(self, criteria):
        clause, vals = self.modify_criteria(criteria)
        res = f"DELETE FROM {self.name}"
        return self._add_where(res, clause), vals
