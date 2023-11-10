import csv, json

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
        
        self.__dtypes = col_dtype_dict
        self.key = key
        self.f_keys = foreign_keys
        self.columns = list(col_dtype_dict.keys())
        self.nrow = 0
        self.ncol = len(col_dtype_dict)
        self.name = name
        
        self.table = {}
        for col in self.__dtypes:
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
                        add = self.__dtypes[col]["cast"](data)
                        if "size" in self.__dtypes[col]:
                            add = add[:self.__dtypes[col]["size"]]
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
        tokens = cmd.split()
        if first_x(tokens, 2) == ["create","table"]:
            name, tbl = create_table(tokens[2:])
            if name:
                TABLES[name] = tbl
        elif first_x(tokens, 2) == ["load","data"]:
            for i in range(len(tokens[2:])-2):
                if tokens[2:][i].lower() == "into" and tokens[2:][i+1].lower() == "table":
                    name = tokens[2:][i+2]
                    break
            if name in TABLES:
                TABLES[name].import_file(tokens[2:])
            else:
                print("ERROR: the table you are trying to load into does not exist")
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

def process_select(cmd):
    print("entered process_select")
    
    tokens = cmd.split()

    cols_dfs_where = [""]
    for tkn in tokens[1:]:
        if tkn == "from" or tkn == "where":
            cols_dfs_where.append("")
        else:
            cols_dfs_where[-1] += tkn+" "

    col_list = [v.strip() for v in cols_dfs_where[0].split(",")]
    dfs_list = [v.strip() for v in cols_dfs_where[1].split(",")]

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
    
    df_aliases = {}
    if len(dfs_list) > 1:
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
    else:
        df_aliases[""] = dfs_list[0].strip()

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

    if len(cols_dfs_where) > 2:
        which_list = [cols_dfs_where[2]]

        ands = ["and","AND"]
        ors = ["or","OR"]

        for delim in ands+ors:
            temp = []
            for string in which_list:
                temp += string.split(f" {delim} ")
            which_list = [e.strip() for e in temp]
        
        arithmetic = ["<=", ">=", "!=", "=", "<", ">"]
        other_conditions = ["not in","in","not like","like"]
        
        print(which_list)




    print(json.dumps(which_columns, indent = 4))
    exit()


def get_input():
    command = ""
    while ";" not in command:
        command += " "+input("> ")
    return [c.strip() for c in command.split(";") if c]

def which_join(data1, data2, col1, col2):
    print("no code yet")

def nested_loop(data1, data2, col1, col2):
    #tuples should return KEYS associated w/ whatever 
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
    #TABLES[data1].sort_table(by=col1)
    #TABLES[data2].sort_table(by=col2)
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
              "insert into df3 (name,Color) values (aac,Orange)"]
        cmd = ["create table df1 (Letter varchar(3), Number int, Color VARCHAR(6), primary key (Letter))",
             "load data infile 'data/df1.csv' into table df1 ignore 1 rows",
             "create table df2 (name varchar(3),decimal float, state varchar(10), year int,  foreign key (name) references df1(Letter), primary key(name))",
             "load data infile 'data/df2.csv' into table df2 ignore 1 rows",
             "select Letter from df1 where Color = 'orange' and Number < 20",
             "select min(a.Letter) as minimum, b.state from df1 a, df2 as b"]
        process_input(cmd)

        #print(nested_loop(TABLES["df1"], TABLES["df3"], "Color", "Color"))
        #print(nested_loop(TABLES["df1"], TABLES["df2"], "Letter", "name"))
        #print(merge_scan(TABLES["df1"], TABLES["df2"], "Letter", "name"))
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