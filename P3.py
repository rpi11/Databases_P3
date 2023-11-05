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
    def __init__(self, col_dtype_dict, key = ""):
        
        self.__dtypes = col_dtype_dict
        self.key = key
        
        self.table = {}
        for col in self.__dtypes:
            self.table[col] = []
        return

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
                if i == 1:
                    header = line

                if i <= ignore:
                    i += 1
                else:
                    for col, data in zip(header, line):
                        add = self.__dtypes[col]["cast"](data)
                        if "size" in self.__dtypes[col]:
                            add = add[:self.__dtypes[col]["size"]]

                        self.table[col].append(add)
            self.size = i - ignore - 1

    def print_table(self, rows = 0):

        print("\t"+"\t".join(list(self.table.keys())))
        column_tup = tuple(self.table[key] for key in self.table)

        for i in range(len(column_tup[0])):
            if rows != 0 and i >= rows:
                break
            print(str(i)+"\t"+"\t".join(str(l[i]) for l in column_tup))
        print()
        return

    def sort_table(self, by = ""):
        by = self.key if by == "" else by

        if by in self.table:
            col_names = list(self.table.keys())
            combine = [tuple(self.table[c][i] for c in col_names) for i in range(self.size)]
            sort = sorted(combine, key = lambda x: x[col_names.index(by)])
        
            for i, c in enumerate(self.table):
                self.table[c] = [t[i] for t in list(sort)]
        else:
            print("specified column to sort by is not in table")

def create_table(cmd):
    cmd = " ".join(cmd)

    i = 0
    while cmd[i] != "(":
        i += 1
    
    name = cmd[:i].strip()
    cols = cmd[i+1:-1].strip().split(",")

    print(cmd)
    print(name)
    print(cols)
    exit()

    columns = {}
    primary_key = ""
    for col in cols:
        if "primary key" in col.lower():
            primary_key = col.split()[-1][1:-1]
        if "foreign key" in col.lower():
            print("FOREIGN")
        else:
            c = col.split()[0]
            d = col.split()[1].replace("("," ").replace(")","").lower().split()

            columns[c] = dtypes[d[0]].copy()
            if len(d) == 2:
                columns[c]["size"] = int(d[1])
    
    return name, Table(columns, primary_key)

def process_input(cmd_list):

    def first_x(tokens, x):
        return [t.lower() for t in tokens[:x]]

    for cmd in cmd_list:
        tokens = cmd.split()
        if first_x(tokens, 2) == ["create","table"]:
            name, tbl = create_table(tokens[2:])
            TABLES[name] = tbl
        if first_x(tokens, 2) == ["load","data"]:
            for i in range(len(tokens[2:])-2):
                if tokens[2:][i].lower() == "into" and tokens[2:][i+1].lower() == "table":
                    name = tokens[2:][i+2]
                    break
            if name in TABLES:
                TABLES[name].import_file(tokens[2:])
            else:
                print("error: the table you are trying to read into does not exist")

        # Parse and handle appropriately

def get_input():
    command = ""
    while ";" not in command:
        command += " "+input("> ")
    return [c.strip() for c in command.split(";") if c]

def main():

    while True:
        # cmd = get_input()
        # if cmd == ["exit"]:
        #     break
        # cmd = ["CREATE TABLE TeSt (state VARCHAR(15),year INT,emissions_per_cap FLOAT,PRIMARY KEY (state))",
        #        "LOAD DATA LOCAL INFILE 'data/emissions.csv' INTO TABLE emissions FIELDS TERMINATED BY ',' IGNORE 1 ROWS"]
        
        cmd = ["create table test (Letter varchar(3), Number int, Color VARCHAR(6)), primary key (Letter))",
               "load data infile 'data/big.csv' into table test ignore 1 rows"]
        process_input(cmd)
        break
        # break

    for name in TABLES:
        TABLES[name].print_table(10)
        TABLES[name].sort_table("Color")
        TABLES[name].print_table(10)
        TABLES[name].sort_table()
        TABLES[name].print_table(10)

    # print("goodbye")
    return 

if __name__ == "__main__":
    main()