import csv, json, time, ast, pandas

TABLES = {}
dtypes = {
    "varchar":{
        "cast":str,
        "size":1
    },
    "float":{
        "cast":float
    },
    "int":{
        "cast":int
    }
}

class Table:
    def __init__(self, name, col_dtype_dict, key = "", foreign_keys = {"":{"table":"","col":""}}):
        
        self.dtypes = col_dtype_dict
        self.key = key
        self.f_keys = foreign_keys
        self.columns = list(col_dtype_dict.keys())
        self.nrow = 0
        self.ncol = len(col_dtype_dict)
        self.name = name
        
        self.table = {}
        for col in self.dtypes:
            self.table[col] = {}
        return

    def insert(self, row_dict):
        if len(row_dict) != self.ncol:
            print(f"ERROR: row insert of length {len(row_dict)} does not match {self.name} column number of {self.ncol}")
            return 1

        for col in row_dict:
            if col in self.f_keys:
                if row_dict[col] not in TABLES[self.f_keys[col]["table"]].table[self.f_keys[col]["col"]]:
                    print(f"ERROR: attempting to insert value {row_dict[col]} that does not exist in foreign key table {self.f_keys[col]['table']}, column {self.f_keys[col]['col']}")
                    return 1
            
            if col not in self.columns:
                print(f"ERROR: insert column {col} does not exist in {self.name}")
                return 1
            
            if col == self.key and row_dict[col] in self.table[col]:
                print(f"ERROR: trying to insert duplicate value {row_dict[col]} into column {col}")
                return 1
            
            if col != self.key:
                if row_dict[col] not in self.table[col]:
                    self.table[col][row_dict[col]] = []
                self.table[col][row_dict[col]].append(row_dict[self.key])

            else:
                self.table[col][row_dict[col]] = {k:v for k,v in row_dict.items() if k != col}

        self.nrow += 1
        return 0

    def get_cols(self):
        return self.columns
    
    def import_file(self, tkns):

        file = ""
        col_delimeter = ","
        line_delimeter = "\n"
        ignore = 0

        for i in range(len(tkns)-2):
            try:
                if tkns[i].lower() == "infile":
                    file = tkns[i+1].replace("'","")
                elif tkns[i].lower() == "ignore" and tkns[i+2].lower() == "rows":
                    ignore = int(tkns[i+1])
                elif [t.lower() for t in tkns[i:i+3]] == ["fields","terminated","by"]:
                    col_delimeter = tkns[i+3][1:-1]
                elif [t.lower() for t in tkns[i:i+3]] == ["lines","terminated","by"]:
                    line_delimeter = tkns[i+3][1:-1]
            except IndexError:
                pass
        
        with open(file, "r") as f:
            reader = csv.reader(f, delimiter=col_delimeter, lineterminator=line_delimeter)
            i = 0
            for line in reader:
                i += 1
                if i > ignore:
                    new_row = {}
                    for col, data in zip(self.columns, line):
                        add = self.dtypes[col]["cast"](data)
                        if "size" in self.dtypes[col]:
                            add = add[:self.dtypes[col]["size"]]
                        new_row[col] = add
                    if self.insert(new_row) == 1:
                        break
        return

    def print_table(self, rows = float("inf")):
        output = []
        for i in range(self.ncol):
            output.append([])

        for i, col in enumerate(self.columns):
            output[i].append(f"<{col}>" if col == self.key else col)
            output[i].append("" if col not in self.f_keys else f"{self.f_keys[col]['table']}({self.f_keys[col]['col']})")
            
            if col == self.key:
                output[i] += list(self.table[self.key].keys())
            else:
                output[i] += [self.table[self.key][k][col] for k in self.table[self.key]]
        
        output = [[" "," "]+list(range(self.nrow))] + output
        max_w = [max(len(str(item)) for item in col) for col in output]

        print(self.name)
        for i in range(self.nrow + 2):
            if i < rows+1:
                print("  ".join(f"{output[j][i]:<{max_w[j]}}" for j in range(self.ncol+1)))
        print()
        return

    def sort_table(self, by = ""):
        by = self.key if by == "" else by
        if by in self.table:
            new_key_order = []
            for key in sorted(list(self.table[by].keys())):
                new_key_order += self.table[by][key]
            self.table[self.key] = {k:self.table[self.key][k] for k in new_key_order}
        else:
            print("ERROR: specified column to sort by is not in table")
        return

def create_table(cmd):
    cmd = " ".join(cmd)

    i = 0
    while cmd[i] != "(":
        i += 1
    
    name = cmd[:i].strip()
    cols = cmd[i+1:-1].replace("("," ").replace(")","").strip().split(",")

    columns = {}
    foreigns = {}
    primary_key = ""
    for col in cols:
        if "primary key" in col.lower():
            primary_key = col.split()[-1]
        elif "foreign key" in col.lower():
            f_col = col.split()[2]
            ref_tbl = col.split()[4]
            ref_col = col.split()[5]

            if f_col not in columns:
                print(f"ERROR: {f_col} column not in table {name}")
                return "",0
            else:
                if ref_tbl not in TABLES:
                    print(f"ERROR: table {ref_tbl} does not exist")
                    return "",0
                else:
                    if ref_col not in TABLES[ref_tbl].columns:
                        print(f"ERROR: {ref_col} column not in table {ref_tbl}")
                        return "",0
                    else:
                        foreigns[f_col] = {
                            "table":ref_tbl,
                            "col":ref_col
                        }
        else:
            c = col.split()[0]
            d = col.lower().split()[1:]

            columns[c] = dtypes[d[0]].copy()
            if len(d) == 2:
                columns[c]["size"] = int(d[1])
    
    return name, Table(name, columns, primary_key, foreigns)

def process_input(cmd_list):
    def first_x(tokens, x):
        return [t.lower() for t in tokens[:x]]

    for cmd in cmd_list:
        start_time = time.time()
        tokens = cmd.split()
        if first_x(tokens, 2) == ["create","table"]:
            name, tbl = create_table(tokens[2:])
            if name:
                TABLES[name] = tbl
        elif first_x(tokens, 2) == ["drop","table"]:
            name = tokens[2]
            TABLES.pop(name)
        elif first_x(tokens, 2) == ["load","data"]:
            for i in range(len(tokens[2:])-2):
                if tokens[2:][i].lower() == "into" and tokens[2:][i+1].lower() == "table":
                    name = tokens[2:][i+2]
                    break
            if name in TABLES:
                TABLES[name].import_file(tokens[2:])
            else:
                print("ERROR: the table you are trying to load into does not exist")
        elif first_x(tokens, 2) == ["create","index"]:
            index = tokens[2]
            if tokens[3].lower() == "on":
                name = tokens[4]
                attribute = tokens[5]
            if name not in TABLES:
                print("ERROR: the table you are trying to create an index on does not exist")
            attribute = attribute[1:-1]
            if attribute not in TABLES[name].columns:
                print("ERROR: the column you are trying to create an index on does not exist")
            # now the actual setting of the index
        elif first_x(tokens, 2) == ["insert","into"]:
            name = tokens[2]
            if name not in TABLES:
                print("ERROR: the table you are trying to insert into does not exist")
            else:
                c_v = ["",""]
                flip = 0
                for i in range(3,len(tokens)):
                    if "values" in tokens[i].lower():
                        flip = 1
                    else:
                        c_v[flip] += tokens[i]
                columns = [e for e in c_v[0].replace("(","").replace(")","").split(",") if e]
                vals = [e for e in c_v[1].replace("(","").replace(")","").split(",") if e]
                if len(columns) != len(vals):
                    print("ERROR: number of insert columns does not match number of insert values")
                else:
                    TABLES[name].insert({c:v for c,v in zip(columns, vals)})
        elif first_x(tokens, 1) == ["select"]:
            process_select(cmd)
        #print("Time for", cmd, ": %s nanoseconds" % round(1000000000*(time.time() - start_time)))       

# region SELECT ########################################################################
def process_select(cmd):
    print(cmd)
    # get columns, dfs, and where condition
    col_list, dfs_list, where, join_list = get_df_col_and_where_list(cmd)

    # get aggregation methods for columns to be gotten
    col_funcs = get_col_funcs(col_list)
    if col_funcs == 1:
        return 1

    # get df alias names
    df_aliases = get_df_aliases(dfs_list)
    if df_aliases == 1:
        return 1

    
    # create column dict that connects aliases
    which_columns = get_which_columns(col_funcs, df_aliases, dfs_list)
    if which_columns == 1:
        return 1

    logic = ""
    cond_columns = {}
    if len(where)>0:
        condition_dict = get_cond_dict(where, df_aliases)
        if condition_dict == 1:
            return 1
        logic = condition_dict["logic"]
        cond_columns = get_cond_columns(condition_dict, df_aliases)

    dfs = list(which_columns.keys())
    if cond_columns:
        dfs += list(cond_columns.keys())
    dfs = list(set(dfs))

    outDict = {}
    for df in dfs:
        outDict[df] = {
            "logic":logic,
            "columns to get":{},
            "subset lists":[]
        }
        if df in which_columns:
            outDict[df]["columns to get"] = which_columns[df]
        
        if df in cond_columns:
            outDict[df]["subset lists"] = [cond_columns[df][cond] for cond in cond_columns[df]]

    #PROCESS JOINS
    #and/or combination here

    #code to join tables
    #will definitely need to be edited based on above but this is the gist for now
    which_join_cols = get_join_cols(join_list, df_aliases)
    if len(dfs_list) > 1:
        #NOTE: something funky w/ sort_tables, will need to look at but for now we'll just do nested
        final_keys = nested_loop(TABLES[dfs[0]], TABLES[dfs[1]], which_join_cols[dfs[0]], which_join_cols[dfs[1]])
        print(final_keys)
    else:
        print("no code yet: see commented note")
        #final_keys = whatever form it'll take after going through 'where' statements, not sure what that will be yet
        #but I'll probably end up formatting it to look like the output for join statements

    return outDict


def get_df_col_and_where_list(cmd):
    tokens = cmd.split()

    cols_dfs_where = [""]
    for tkn in tokens[1:]:
        if tkn == "from" or tkn == "join" or tkn == "where":
            cols_dfs_where.append("")
        else:
            cols_dfs_where[-1] += tkn+" "

    col_list = [v.strip() for v in cols_dfs_where[0].split(",")]
    dfs_list = [v.strip() for v in cols_dfs_where[1].split(",")]
    if len(cols_dfs_where) > 2:
        #join_list = [cols_dfs_where[2].strip()]
        join_list = [v.strip() for v in cols_dfs_where[2].split(" ")]
    else:
        join_list = []
    print(join_list)

    if len(cols_dfs_where) > 3:
        where_list = [cols_dfs_where[3].strip()]
    else:
        where_list = []

    return col_list, dfs_list, where_list, join_list

def get_col_funcs(col_list):
    col_funcs = {}
    for col in col_list:
        name = col
        agg = ""
        alias = name
        if "(" in col:
            agg = col.split("(")[0].lower().strip()
            if agg not in ["min","max","sum","average","mode"]:
                print(f"ERROR: aggregation method {agg} not supported")
                return 1
            name = col.split("(")[1].split(")")[0].strip()
        
        for tp in ["as","AS","As"]:
            if f" {tp} " in col:
                alias = col.split(tp)[-1].strip()
                break
        
        col_funcs[name] = {
            "agg":agg,
            "alias":alias
        }
    return col_funcs

def get_df_aliases(dfs_list):
    df_aliases = {}
    for df in dfs_list:
        alias = ""
        if any(f" {tp} " in df for tp in ["as","AS"]):
            for tp in ["as","AS"]:
                if f" {tp} " in df:
                    alias = df.split(tp)[-1].strip()
                    name = "".join(df.split(tp)[:-1]).strip()
                    break
        else:
            alias = df.split()[-1].strip()
            if len(df.split()) > 1:
                name = "".join(df.split()[:-1]).strip()
            else:
                name = "".join(df.split()[0]).strip()
        df_aliases[alias] = name

    return df_aliases

def get_which_columns(col_funcs, df_aliases, dfs_list):
    which_columns = {}
    for col in col_funcs:
        if "." in col:
            alias = col.split(".")[0]
            if alias in df_aliases:
                if df_aliases[alias] not in which_columns:
                    which_columns[df_aliases[alias]] = {}
                which_columns[df_aliases[alias]][col.split(".")[1]] = col_funcs[col]
            else:
                print(f"ERROR: alias {alias} not assigned to a table")
                return 1
        else:
            if dfs_list[0] not in which_columns:
                which_columns[dfs_list[0]] = {}
            which_columns[dfs_list[0]][col] = col_funcs[col]

    for df in which_columns:
        if df not in TABLES:
            print(f"ERROR: table {df} does not exist")
            return 1
        else:
            if "*" in which_columns[df]:
                which_columns[df] = {c:{"agg":"","alias":c} for c in TABLES[df].columns}
            else:
                for col in which_columns[df]:
                    if col not in TABLES[df].columns:
                        print(f"ERROR: column {col} does not exist in table {df}")
                        return 1
    return which_columns 

def get_join_cols(join_list, df_aliases):
    which_join_cols = {}
    for x in join_list:
        if "." in x:
            alias = x.split(".")[0]
            print("alias: " + alias)
            if alias in df_aliases:
                if df_aliases[alias] not in which_join_cols:
                    which_join_cols[df_aliases[alias]] = {}
                temp = x.split(".")[1]
                which_join_cols[df_aliases[alias]] = temp.split(" ")[0]
            else:
                print(f"ERROR: alias {alias} not assigned to a table")
                return 1

    return which_join_cols

def get_cond_dict(where, df_aliases):
    condition_dict = {
        "logic":"",
        "arithmetic":{},
        "string":{
            "ins":{},
            "likes":{}
        }
    }

    ands = ["and","AND"]
    ors = ["or","OR"]

    for delim in ands+ors:
        temp = []
        for string in where:
            if f" {delim} " in string:
                condition_dict["logic"] = delim.lower()
            temp += string.split(f" {delim} ")
        where = [e.strip() for e in temp]

    def def_col_error(df_col):
        if df_col[0] not in df_aliases:
            if df_col == "":
                print(f"ERROR: no dataframe referenced with variable {df_col[1]}")
            else:
                print(f"ERROR: df alias {df_col[0]} does not exist")
            return True
        elif df_col[1] not in TABLES[df_aliases[df_col[0]]].columns:
            print(f"ERROR: column {df_col[1]} not in df {df_aliases[df_col[0]]}")
            return True
        return False

    arithmetic = ["<=", ">=", "!=", "==", "<", ">"]
    ins = ["not in","in"]
    likes = ["not like","like"]
    for cond in where:
        if any([a in cond for a in arithmetic]):
            for a in arithmetic:
                if a in cond:
                    # Too hard without regex to deal with decimals on left
                    # side of operator so we just assume integers on left
                    modified_cond = cond.split(a)
                    modified_cond = modified_cond[0].replace(".","___")+a+modified_cond[1]
                    condition_dict["arithmetic"][modified_cond] = {}

                    parsed = ast.parse(modified_cond, mode = "exec")
                    variable_names = [n.id for n in ast.walk(parsed) if isinstance(n, ast.Name)]
                    for v in variable_names:
                        
                        if "___" in v:
                            df_col = v.split("___")
                        else:
                            df_col = ["",v]
                        
                        if def_col_error(df_col):
                            return 1

                        condition_dict["arithmetic"][modified_cond][v] = {
                            "df_alias":df_col[0],
                            "column":df_col[1]
                        }
                    break
        elif any([f" {s} " in cond for s in ins+[e.upper() for e in ins]]):
            for s in ins+[e.upper() for e in ins]:
                if f" {s} " in cond:
                    col_list = [e.strip() for e in cond.split(f" {s} ")]

                    if "." in col_list[0]:
                        df_col = col_list[0].split(".")
                    else:
                        df_col = ["",col_list[0]]
                    if def_col_error(df_col):
                        return 1

                    dtp = TABLES[df_aliases[df_col[0]]].dtypes[df_col[1]]["cast"]
                    try:
                        check_list = [dtp(e.strip()) for e in col_list[1].\
                                        replace("(","").replace(")","").replace("'","").replace('"',"").split(",")]
                    except ValueError:
                        print(f"ERROR: cannot convert values in list {col_list[1]} to type {dtp}")
                        return 1

                    condition_dict["string"]["ins"][cond] = {
                        "df_alias":df_col[0],
                        "columns":df_col[1],
                        "eval":s.lower() == "in",
                        "list":check_list
                    }
                    break
        elif any([s in cond for s in likes+[e.upper() for e in likes]]):
            for s in likes+[e.upper() for e in likes]:
                if f" {s} " in cond:
                    col_string = [e.strip() for e in cond.split(f" {s} ")]
                    compare = col_string[1].replace("'","").replace('"',"")
                    
                    if "." in col_string[0]:
                        df_col = col_string[0].split(".")
                    else:
                        df_col = ["",col_string[0]]
                    if def_col_error(df_col):
                        return 1
                    
                    condition_dict["string"]["likes"][cond] = {
                        "df_alias":df_col[0],
                        "columns":df_col[1],
                        "eval":s.lower() == "like",
                    }

                    comp_str = compare
                    if compare[0] == "%" and compare[-1] == "%":
                        condition_dict["string"]["likes"][cond]["type"] = "within"
                        comp_str = comp_str[1:-1]
                    elif compare[-1] == "%":
                        condition_dict["string"]["likes"][cond]["type"] = "start"
                        comp_str = comp_str[:-1]
                    else:
                        condition_dict["string"]["likes"][cond]["type"] = "end"
                        comp_str = comp_str[1:]
                    condition_dict["string"]["likes"][cond]["compare"] = comp_str
                    break
        else:
            print(f"ERROR: invalid conditional statement in {cond}")
            return 1
        
    return condition_dict

def get_cond_columns(c, df_aliases):

    # arithmetic
    cond_list = {}
    for cond in c["arithmetic"]:
        df = ""
        for var in c["arithmetic"][cond]:
            if df and df != df_aliases[c["arithmetic"][cond][var]["df_alias"]]:
                print(f"ERROR: trying to perform subsetting with condition that references multiple tables")
            elif not df:
                df = df_aliases[c["arithmetic"][cond][var]["df_alias"]]

        if df not in cond_list:
            cond_list[df] = {}
        cond_back = cond.replace("___",".")
        cond_list[df][cond_back] = []

        parsed = ast.parse(cond, mode='eval')
        if len(c["arithmetic"][cond]) == 1: # Can just condition if only one variable is considered
            var = list(c["arithmetic"][cond].keys())[0]
            column = c["arithmetic"][cond][var]["column"]
            isKey = (column == TABLES[df].key)

            for val in TABLES[df].table[column]:
                if eval(compile(parsed, filename='<string>', mode='eval'), {}, {var:val}):
                    if isKey:
                        cond_list[df][cond_back].append(val)
                    else:
                        cond_list[df][cond_back].extend(TABLES[df].table[column][val])
            cond_list[df][cond_back] = list(set(cond_list[df][cond_back]))
        else: # Otherwise we need to actually just scan each value
            var_col_dict = {var:c["arithmetic"][cond][var]["column"] for var in c["arithmetic"][cond]}
            
            for key in TABLES[df].table[TABLES[df].key]:
                params = {var:TABLES[df].table[TABLES[df].key][key][var_col_dict[var]] for var in var_col_dict}
                if eval(compile(parsed, filename='<string>', mode='eval'), {}, params):
                    cond_list[df][cond_back].append(key)

    # ins
    for cond in c["string"]["ins"]:
        df = df_aliases[c["string"]["ins"][cond]["df_alias"]]
        if df not in cond_list:
            cond_list[df] = {}
        cond_list[df][cond] = []

        column = c["string"]["ins"][cond]["columns"]
        isKey = (column == TABLES[df].key)
        if c["string"]["ins"][cond]["eval"]:
            for val in c["string"]["ins"][cond]["list"]:
                if val in TABLES[df].table[column]:
                    if isKey:
                        cond_list[df][cond].append(val)
                    else:
                        cond_list[df][cond].extend(TABLES[df].table[column][val])
        else:
            for val in TABLES[df].table[column]:
                if val not in c["string"]["ins"][cond]["list"]:
                    if isKey:
                        cond_list[df][cond].append(val)
                    else:
                        cond_list[df][cond].extend(TABLES[df].table[column][val])

        cond_list[df][cond] = list(set(cond_list[df][cond]))

    # likes
    for cond in c["string"]["likes"]:
        df = df_aliases[c["string"]["likes"][cond]["df_alias"]]
        column = c["string"]["likes"][cond]["columns"]
        isKey = (column == TABLES[df].key)
        cond_list[df][cond] = []

        compare = c["string"]["likes"][cond]["compare"]
        tp = c["string"]["likes"][cond]["type"]

        if tp == "within":
            for val in TABLES[df].table[column]:
                if (compare in val) == c["string"]["likes"][cond]["eval"]:
                    if isKey:
                        cond_list[df][cond].append(val)
                    else:
                        cond_list[df][cond].extend(TABLES[df].table[column][val])
        elif tp == "end":
            for val in TABLES[df].table[column]:
                if val.endswith(compare) == c["string"]["likes"][cond]["eval"]:
                    if isKey:
                        cond_list[df][cond].append(val)
                    else:
                        cond_list[df][cond].extend(TABLES[df].table[column][val])
        elif tp == "start":
            for val in TABLES[df].table[column]:
                if val.startswith(compare) == c["string"]["likes"][cond]["eval"]:
                    if isKey:
                        cond_list[df][cond].append(val)
                    else:    
                        cond_list[df][cond].extend(TABLES[df].table[column][val])

    return cond_list




# endregion SELECT #####################################################################

# region OPTIMIZATION ########################################################################
def get_input():
    command = ""
    while ";" not in command:
        command += " "+input("> ")
    return [c.strip() for c in command.split(";") if c]

def which_join(data1, data2, col1, col2):
    #if (both tables sorted: add sorted attribute later):
    #    return merge_scan(data1, data2, col1, col2)
    if len(data1.table[col1]) > len(data2.table[col2]):
        d1 = data1
        d2 = data2
        c1 = col1
        c2 = col2
    else:
        d1 = data2
        d2 = data1
        c1 = col2
        c2 = col1
    if len(d1.table[c1]) > 10000:
        if len(d2.table[c2]) > 50:
            d1.sort_table(by=c1)
            d2.sort_table(by=c2)
            return merge_scan(d1, d2, c1, c2)
        else:
            #if longer table is sorted:
            #    d2.sort_table(by=c2)
            #    return merge_scan(d1, d2, c1, c2)
            #else:
            return nested_loop(d1, d2, c1, c2)
    

def nested_loop(data1, data2, col1, col2):
    keys1 = []
    keys2 = []
    if data1.nrow < data2.nrow:
        for i in data1.table[col1]:
            for j in data2.table[col2]:
                if i == j:
                    if col1 == data1.key:
                        keys1.append(i)
                    else:
                        temp = data1.table[col1]
                        keys1.append(temp[i])
                    if col2 == data2.key:
                        keys2.append(j)
                    else:
                        temp = data2.table[col2]
                        keys2.append(temp[i])
    else:
        for j in data2.table[col2]:
            for i in data1.table[col1]:
                if i == j:
                    if col1 == data1.key:
                        keys1.append(i)
                    else:
                        temp = data1.table[col1]
                        keys1.append(temp[i])
                    if col2 == data2.key:
                        keys2.append(j)
                    else:
                        temp = data2.table[col2]
                        keys2.append(temp[i])
    return [keys1, keys2]

def merge_scan(data1, data2, col1, col2):
    values1 = list(data1.table[col1].keys())
    values2 = list(data2.table[col2].keys())
    keys1 = []
    keys2 = []
    i = 0
    j = 0
    #data1.sort_table(by=col1)
    #data2.sort_table(by=col2)
    while i < len(data1.table[col1]) and j < len(data2.table[col2]):
        if values1[i] < values2[j]:
            i = i + 1
        elif values1[i] > values2[j]:
            j = j + 1
        else:
            if col1 == data1.key:
                keys1.append(values1[i])
            else:
                temp = data1.table[col1]
                keys1.append(temp[values1[i]])
            if col2 == data2.key:
                keys2.append(values2[j])
            else:
                temp = data2.table[col2]
                keys2.append(temp[values2[j]])
            i = i + 1
            j = j + 1
    return [keys1, keys2]

def and_optimizer(dfs_list, col_list, which_list):
    selectivity = [[]]
    selectivity[0] = which_list
    for condition, i in selectivity[0]:
        # run text as conditioning and calculate selectivity index
        # how is each condition being processed -- final form of sorted list?
        temp = []
        for word in condition.split(" "):
            temp.append(word.lower())
        if temp[0] in TABLES[dfs_list].columns:
            selectivity_index = TABLES[dfs_list].loc[temp[0] == some_value, 'col2'].sum()/sum(TABLES[dfs_list].loc[temp[0]])
            selectivity[1][i] = selectivity_index
        else:
            print("column conditioning on does not exist")

    selectivity_sorted = selectivity[0].sort(key = lambda x: x[1], reverse = True)
    return selectivity_sorted


def query_tree(cmd):
    print("no code yet")

# endregion OPTIMIZATIONS #####################################################################

def main():

    while True:
        # cmd = get_input()
        # if cmd == ["exit"]:
        #     break
        # cmd = ["CREATE TABLE TeSt (state VARCHAR(15),year INT,emissions_per_cap FLOAT,PRIMARY KEY (state))",
        #        "LOAD DATA LOCAL INFILE 'data/emissions.csv' INTO TABLE emissions FIELDS TERMINATED BY ',' IGNORE 1 ROWS"]

        # 
        cmd = ["create table df1 (Letter varchar(3), Number int, Color VARCHAR(6), primary key (Letter))",
               "load data infile 'data/df1.csv' into table df1 ignore 1 rows",
               "create table df2 (decimal float, state varchar(10), year int, name varchar(3), foreign key (name) references df1(Letter), primary key(name))",
               "insert into df2 (name,decimal,state,year) values (aab,0.2,Minnesota,2002)",
               "insert into df2 (name,decimal,state,year) values (aao,0.4,Minnesota,2004)", 
               "create table df3 (name varchar(3), Color VARCHAR(6), primary key (name))",
               "insert into df3 (name,Color) values (aab,Red)",
               "insert into df3 (name,Color) values (aad,Red)",
               "insert into df3 (name,Color) values (aac,Orange)",
               "select a.Letter, b.name from df1 a, df2 b join a.Letter = b.name",
            "select a.Letter, b.name from df1 a, df2 b join a.Letter = b.name where a.Number > 50"]
        #cmd = ["create table df1 (Letter varchar(3), Number int, Color VARCHAR(6), primary key (Letter))",
        #   "load data infile 'data/df1.csv' into table df1 ignore 1 rows",
        #   "create table df2 (name varchar(3),decimal float, state varchar(10), year int, foreign key (name) references df1(Letter), primary key(name))",
        #   "load data infile 'data/df2.csv' into table df2 ignore 1 rows",
           #"select b.name, min(b.decimal) from df2 as b where b.name not like 'aa%' and b.decimal*2<.05 and b.state <= 'Alabama' and (b.decimal*800) + b.year < 1910 and b.state in ('Iowa','Minnesota','Indiana')",
           #"select a.Letter, max(a.Number) from df1 as a where a.Letter not like 'aa%' and a.Number*2 < 20 and a.Number + a.Number < 30 and a.Color in ('Orange','Yellow','Blue')",
           # "select min(a.Letter) as minimum, b.state from df1 a, df2 as b where a.Letter == b.name and b.decimal in (1,2,3,4)",
           # "select a.Letter, b.name from df1 a, df2 b join a.Letter = b.name",
           # "select a.Letter, b.name from df1 a, df2 b join a.Letter = b.name where a.Number > 50",
           # ]
        process_input(cmd)

        #x = nested_loop(TABLES["df1"], TABLES["df3"], "Color", "Color")
        #output(TABLES["df1"], TABLES["df3"], x)
        #nested_loop(TABLES["df1"], TABLES["df2"], "Letter", "name")
        #print(merge_scan(TABLES["df1"], TABLES["df2"], "Letter", "name"))
        # print(which_join(TABLES["df1"], TABLES["df2"], "Letter", "name"))
        # print(merge_scan(TABLES["df1"], TABLES["df3"], "Color", "Color"))
        # cmd2 = ["select test1, test2, test3 from test4, test5 where"]
        # process_input(cmd2)
        break
        # break
    #TABLES["df2"].sort_table(by="name")
    #TABLES["df1"].print_table()
    #TABLES["df3"].print_table()

    #for name in TABLES:
    #    TABLES[name].sort_table(by="Number")
    #    TABLES[name].print_table()
    #    break

    # print("goodbye")
    return 

if __name__ == "__main__":
    main()