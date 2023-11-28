import csv, json, time, ast, math, sys
from prettytable import PrettyTable

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
    """
    Class definition for each table that will exist within out relation
    """

    def __init__(self, name, col_dtype_dict, key = "", foreign_keys = {"":{"table":"","col":""}}):
        """
        Initialization function for table

        Params:
            name: name of table
            col_dtype_dict: dictionary of datatypes for each column
            key: the column name that will be the key of the table
            foreign_keys: a dictionary that will map columns to foreign column that they refer to
        """
        self.dtypes = col_dtype_dict
        self.key = key
        self.f_keys = foreign_keys

        # For each foreign key, add to that column's table's dictionary
        # of "child columns." This is useful for cascading deletion
        for col in foreign_keys:
            if col:
                TABLES[foreign_keys[col]["table"]].child_keys = {
                    foreign_keys[col]["col"]:{
                        "table":name,
                        "col":col
                    }
                }
        self.child_keys = {}

        self.columns = list(col_dtype_dict.keys())
        self.nrow = 0
        self.ncol = len(col_dtype_dict)
        self.name = name
        
        self.table = {}
        for col in self.dtypes:
            self.table[col] = {}
        return

    def insert(self, row_dict):
        """
        Member function for insertion into the table. This will insert
        a single row element, which can then be chained for larger importing.

        Params:
            row_dict: the column:val dictionary for the row being inserted
        
        Return:
            1 if error else 0
        """

        # Check if length of row dictionary is the same as number of columns
        if len(row_dict) != self.ncol:
            print(f"ERROR: row insert of length {len(row_dict)} does not match {self.name} column number of {self.ncol}")
            return 1
        
        for col in row_dict:

            try:
                row_dict[col] = self.dtypes[col]["cast"](row_dict[col])
            except ValueError:
                print(f"ERROR: cannot convert value {row_dict[col]} to type {self.dtypes[col]['cast']}")
                return 1

            # If the column is a foreign key, make sure that 
            # no duplicates exist in the referred-to column
            if col in self.f_keys:
                if row_dict[col] not in TABLES[self.f_keys[col]["table"]].table[self.f_keys[col]["col"]]:
                    print(f"ERROR: attempting to insert value {row_dict[col]} that does not exist in foreign key table {self.f_keys[col]['table']}, column {self.f_keys[col]['col']}")
                    return 1
            
            # Make sure that the column exists in the table
            if col not in self.columns:
                print(f"ERROR: insert column {col} does not exist in {self.name}")
                return 1
            
            # If the column is this table's key, make sure that
            # no duplicates exist in the column
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

    def empty(self):
        self.table = {col:{} for col in self.columns}
        for col in self.child_keys:
            TABLES[self.child_keys[col]["table"]].empty()

    def import_file(self, tkns):
        """
        Member function for importing from a file

        Params: 
            tkns: the tokenized version of the input command
        """

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
        
        # Read in each line of the file and then
        # use the insert command to add them to the table
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

    def update(self, tokens):
        """
        Member function for updating values in the table.

        Params:
            tkns: the tokenized version of the input command
        """

        cmd_sects = {
            "df":"",
            "set":"",
            "where":""
        }
        key = ""
        for t in tokens:
            if t.lower() == "update":
                key = "df"
            elif t.lower() == "set":
                key = "set"
            elif t.lower() == "where":
                key = "where"
            else:
                cmd_sects[key] += t + " "
        
        cols = [c.split("=")[0].strip() for c in cmd_sects["set"].split(",")]
        
        if any([c == self.key for c in cols]):
            # Checks to make sure you are not trying to update a primary key column
            print("ERROR: trying to update value in primary key column")
        else:
            # Get the values that need to be assigned
            assigns = [c.split("=")[1].strip() for c in cmd_sects["set"].split(",")]
            for i in range(len(assigns)):
                try:
                    assigns[i] = self.dtypes[cols[i]]["cast"](assigns[i])
                    cont = True
                except ValueError:
                    cont = False
                    print(f"ERROR: cannot convert value of type {type(assigns[i])} to {self.dtypes[cols[i]]['cast']}")

            
            # If the datatype conversion is acceptable, then actually update
            if cont:  
                # This part basically just runs a select-query and then
                # gets the keys from thr returned table to then update
                assign_dict = {old:new for old, new in zip(cols, assigns)}
                select = f"select {', '.join(cols)} from {cmd_sects['df'].strip()} where {cmd_sects['where'].strip()}"
                subset_vals = process_select(select, do_print=False)

                # For each column in the returned, conditioned table...
                for col in assign_dict:

                    # If the update value is not in the table-column index,
                    # create an empty table
                    if assign_dict[col] not in self.table[col]:
                        self.table[col][assign_dict[col]] = []

                    # For each value in the subset values,
                    # add the associated keys to the list for the
                    # replacement value and then pop the value
                    # from the column index if it is not the replacement value.
                    for val in subset_vals[col]:
                        self.table[col][assign_dict[col]] += self.table[col][val].copy()
                        if val != assign_dict[col]:
                            self.table[col].pop(val)
                    
                    # For each key in that new list, make sure that the 
                    # corresponding value in the primary key index is
                    # also correctly updated.
                    for key in self.table[col][assign_dict[col]]:
                        self.table[self.key][key][col] = assign_dict[col]

    def delete(self, tokens):
        """
        Member function for deleting values in the table. This
        incorporates cascading deletion.

        Params:
            tokens: the tokenized version of the input command
        """

        # Similar to update, just processes the conditional statement
        # with the selection function and then handles the returned keys
        where = " ".join(tokens[2:]).split(",")
        select_command = f"select {self.key} from {self.name} where {', '.join(where)}"
        keys = process_select(select_command, False)

        # For each key in the key column of the delete-list
        for key in keys[self.key]:
            # For each column in the table
            for col in self.table[self.key][key]:
                val = self.table[self.key][key][col]

                # remove the key from the corresponding value list
                self.table[col][val].remove(key)

                # If the column has a child-list,
                # then call the delete member function 
                # for the table that references
                if col in self.child_keys:
                    tbl = self.child_keys[col]["table"]
                    c = self.child_keys[col]["col"]

                    TABLES[tbl].delete([tbl,"where",c,"==",str(val)])
            
            # If the key column has a child-list,
            # do the same as with the columns.
            if self.key in self.child_keys:
                tbl = self.child_keys[self.key]["table"]
                c = self.child_keys[self.key]["col"]
                TABLES[tbl].delete([tbl,"where",c,"==",str(val)])
            
            # Remove the key value from the primary-key column
            self.table[self.key].pop(key)

    # def print_table(self, rows = float("inf")):
    #     output = []
    #     for i in range(self.ncol):
    #         output.append([])

    #     for i, col in enumerate(self.columns):
    #         output[i].append(f"<{col}>" if col == self.key else col)
    #         output[i].append("" if col not in self.f_keys else f"{self.f_keys[col]['table']}({self.f_keys[col]['col']})")
            
    #         if col == self.key:
    #             output[i] += list(self.table[self.key].keys())
    #         else:
    #             output[i] += [self.table[self.key][k][col] for k in self.table[self.key]]
        
    #     output = [[" "," "]+list(range(self.nrow))] + output
    #     max_w = [max(len(str(item)) for item in col) for col in output]

    #     print(self.name)
    #     for i in range(self.nrow + 2):
    #         if i < rows+1:
    #             print("  ".join(f"{output[j][i]:<{max_w[j]}}" for j in range(self.ncol+1)))
    #     print()
    #     return

    # def sort_table(self, by = ""):
    #     by = self.key if by == "" else by
    #     if by in self.table:
    #         new_key_order = []
    #         for key in sorted(list(self.table[by].keys())):
    #             new_key_order += self.table[by][key]
    #         self.table[self.key] = {k:self.table[self.key][k] for k in new_key_order}
    #     else:
    #         print("ERROR: specified column to sort by is not in table")
    #     return

def create_table(cmd):
    """
    Function to create a table

    Params:
        cmd: the tokenized command to create the table
    """
    
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
        # if the primary key is in the 
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
            if name in TABLES:
                TABLES[name].empty()
                TABLES.pop(name)
            else:
                print("ERROR: the table you are trying to load into does not exist")
            
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
                    print("ERROR: number of insert columns does not match number of insert values, check insert syntax")
                else:
                    TABLES[name].insert({c:v for c,v in zip(columns, vals)})
        elif first_x(tokens, 1) == ["select"]:
            process_select(cmd)
        elif first_x(tokens, 1) == ["update"]:
            name = tokens[1]
            if name not in TABLES:
                print("ERROR: the table you are trying to insert into does not exist")

            if not all([c in [e.lower() for e in tokens] for c in ["set", "where"]]):
                print("ERROR: update query not properly formatted")
            else:
                TABLES[name].update(tokens)
        elif first_x(tokens, 2) == ["delete","from"]:
            name = tokens[2]
            if name not in TABLES:
                print("ERROR: the table you are trying to insert into does not exist")
            
            if "where" not in [e.lower() for e in tokens]:
                print("ERROR: deletion query not properly formatted")
            else:
                TABLES[name].delete(tokens[2:])


        print("Time for", cmd, ": %s nanoseconds" % round(1000000000*(time.time() - start_time)))     

# region SELECT ########################################################################
def process_select(cmd, do_print = True):

    # get columns, dfs, and where condition
    col_list, dfs_list, where, join_list = get_df_col_and_where_list(cmd)
    # get aggregation methods for columns to be gotten
    col_funcs = get_col_funcs(col_list)
    if col_funcs == 1:
        return 1
    if len(col_list) > 1 and list(col_funcs.values())[0]['agg'] != "":
        print("ERROR: You cannot output more than one column with an aggregation function.")
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
    dfs = []
    for x in dfs_list:
        dfs.append(x.split()[0])
    #print(dfs)
    #dfs = list(which_columns.keys())
    #if cond_columns:
    #    dfs += list(cond_columns.keys())
    #dfs = list(set(dfs))
    conjunctive = True
    if logic == "or" or logic == "OR":
        conjunctive = False
    elif logic == "and" or logic == "AND":
        conjunctive = True

    outDict = {}
    for df in dfs:
        outDict[df] = {
            "logic":logic,
            "columns to get":{},
            "subset lists":[],
            "subsetted":False
        }
        if df in which_columns:
            outDict[df]["columns to get"] = which_columns[df]
        
        #move and optimizer here
        if df in cond_columns:
            outDict[df]["subset lists"] = [cond_columns[df][cond] for cond in cond_columns[df]]
            outDict[df]["subsetted"] = True
    # print(json.dumps(cond_columns, indent = 4))
    #PROCESS JOINS
    #and/or combination here

    for df in dfs:
        if len(outDict[df]["subset lists"]) > 1:
            outDict[df]["subset lists"] = and_optimizer(cond_columns) if logic=="and" else or_optimizer(cond_columns)
        elif len(outDict[df]["subset lists"]) == 1:
            new_subset_lists = []
            for l in outDict[df]["subset lists"]:
                new_subset_lists.extend(l)
            outDict[df]["subset lists"] = new_subset_lists

    #okay, so we've outputted data from both of the tables, now I want to join what we've printed above:
    join_cols = get_join_cols(join_list, df_aliases)
    if logic == "and" or logic == "AND":
        for df in dfs:
            whiled = False
            while len(outDict[df]["subset lists"]) > 1:
                whiled = True
                joined_cond_lists = which_join(df, df, outDict[df]["subset lists"][0], outDict[df]["subset lists"][1], join_cols[df], join_cols[df], conjunctive)
                joined_cond_lists = [v for v in joined_cond_lists.values()]
                #joined_cond_lists = which_join(df, df, outDict[df]["subset lists"][0], outDict[df]["subset lists"][1], TABLES[df].key, TABLES[df].key, conjunctive)
                outDict[df]["subset lists"][0] = joined_cond_lists[0]
                outDict[df]["subset lists"].remove(outDict[df]["subset lists"][1])
            if whiled:
                new_subset_lists = []
                for l in outDict[df]["subset lists"]:
                    new_subset_lists.extend(l)
                outDict[df]["subset lists"] = new_subset_lists
  
    #code to join tables (if necessary)
    if len(dfs_list) > 1:
        if outDict[dfs[0]]["subsetted"] is True:
            temp1 = outDict[dfs[0]]["subset lists"]
        else:
            temp1 = list(TABLES[dfs[0]].table[(TABLES[dfs[0]].key)].keys())
            #temp1 = TABLES[dfs[0]].table[col1]
        if outDict[dfs[1]]["subsetted"] is True:
            temp2 = outDict[dfs[1]]["subset lists"]
        else:
            temp2 = list(TABLES[dfs[1]].table[(TABLES[dfs[1]].key)].keys())
            #print(i.values())
        #final_keys = which_join(dfs[0], dfs[1], temp1, temp2, TABLES[dfs[0]].key, TABLES[dfs[1]].key, conjunctive)
        final_keys = which_join(dfs[0], dfs[1], temp1, temp2, join_cols[dfs[0]], join_cols[dfs[1]], conjunctive)
        #print(final_keys)
        #test_time = time.time()
        # if conjunctive:
        #     temp = final_keys[0]
        #     for k in final_keys[1]:
        #         temp.append(k)
        #     temp = sorted(temp)
        #     i = 0
        #     j = 0
        #     temp2 = []
        #     while i < len(temp) - 1:
        #         if temp[i] == temp[i + 1]:
        #             if j == 0:
        #                 j = i
        #                 temp2.append(temp[i])
        #             else:
        #                 if temp[i] != temp[j]:
        #                     temp2.append(temp[i])
        #         i = i + 1
        #     final_keys = temp2
        # else:
        #     temp = final_keys[0]
        #     for k in final_keys[1]:
        #         temp.append(k)
        #     temp = sorted(temp)
        #     i = 0
        #     while i < len(temp) - 1:
        #         if temp[i] == temp[i + 1]:
        #             temp.remove(temp[i])
        #         else:
        #             i = i + 1
        #     final_keys = temp
        #print(round(1000000000*(time.time() - test_time)))
    else:
        if outDict[dfs[0]]["subsetted"] is True:
            final_keys = {dfs[0]:outDict[dfs[0]]["subset lists"]}
        else:
            final_keys = {dfs[0]:list(TABLES[dfs[0]].table[TABLES[dfs[0]].key].keys())}
            
    #FINAL OUTPUT!
    final_output = {}
    #Handle aggregation operators (if any)
    agg = False
    for x in col_funcs.values():
        if x['agg'] != "":
            i = 0
            agg = True
            df = dfs[0]
            column = list(outDict[df]["columns to get"].keys())[0]
            final_keys = final_keys[df]
            final_output[column] = []
            for c in TABLES[df].columns:
                if c == column:
                    break
                i = i + 1
            if x['agg'].lower() == "min":
                if column == TABLES[dfs[0]].key:
                    minimum = min(final_keys)
                else:
                    minimum = find_data_type(dfs[0], column, "min")
                    for k in final_keys:
                        if TABLES[df].table[TABLES[df].key][k][column] < minimum:
                            minimum = TABLES[df].table[TABLES[df].key][k][column]
                final_output[column].append(minimum)
            elif x['agg'].lower() == "max":
                if column == TABLES[dfs[0]].key:
                    maximum = max(final_keys)
                else:
                    maximum = find_data_type(dfs[0], column, "max")
                    for k in final_keys:
                        if TABLES[df].table[TABLES[df].key][k][column] > maximum:
                            maximum = TABLES[df].table[TABLES[df].key][k][column]
                final_output[column].append(maximum)
            elif x['agg'].lower() == "avg":
                average = find_data_type(dfs[0], column, "avg")
                if average != 1:
                    j = 0
                    for k in final_keys:
                        average = average + TABLES[df].table[TABLES[df].key][k][column]
                        j = j + 1
                    average = average / j
                    final_output[column].append(average)
            elif x['agg'].lower() == "sum":
                sum_ = find_data_type(dfs[0], column, "sum")
                if sum_ != 1:
                    for k in final_keys:
                        sum_ = sum_ + TABLES[df].table[TABLES[df].key][k][column]
                    final_output[column].append(sum_)
                
    #Finds final output if there are no aggregation operators 
    if agg is False:
        for df in dfs:
            for column in list(outDict[df]["columns to get"].keys()):
                if column != TABLES[df].key:
                    final_output[df+"_"+column] = [TABLES[df].table[TABLES[df].key][k][column] for k in final_keys[df]]
                else:
                    final_output[df+"_"+column] = [k for k in final_keys[df]]
                

        # if len(dfs_list) > 1:
        #     for df in dfs:
        #         for column in TABLES[df].columns:
        #             if column in list(outDict[df]["columns to get"].keys()):
        #                 for k in final_keys:
        #                     if column == TABLES[df].key:
        #                         if column not in final_output:
        #                             final_output[column] = []
        #                         final_output[column].append(k)
        #                     else:
        #                         temp = TABLES[df].table[TABLES[df].key]
        #                         #print(temp)
        #                         temp2 = temp[k]
        #                         if column not in final_output:
        #                             final_output[column] = []
        #                         final_output[column].append(temp2[column])
        # else:
        #     for column in TABLES[dfs[0]].columns:
        #         if column in list(outDict[dfs[0]]["columns to get"].keys()):
        #             for k in final_keys:
        #                 if column == TABLES[dfs[0]].key:
        #                     if column not in final_output:
        #                         final_output[column] = []
        #                     final_output[column].append(k)
        #                 else:
        #                     temp = TABLES[dfs[0]].table[TABLES[dfs[0]].key]
        #                     temp2 = temp[k]
        #                     if column not in final_output:
        #                         final_output[column] = []
        #                     final_output[column].append(temp2[column])
    print(final_output)
    if do_print:
        print_output(final_output)
    
    return final_output

def print_output(final_output):
    #Takes the final output and prints it for the user (last step!)
    table = PrettyTable()

    table.field_names = list(final_output.keys())

    final_rows = list(map(list, zip(*final_output.values())))

    for row in final_rows:
        table.add_row(row)
    print(table)

def find_data_type(df, c, string):
    #Used for aggregation operators: gets the datatype of the operator and returns appropriate type and value
    for column in TABLES[df].dtypes:
        if c == column:
            if TABLES[df].dtypes[column]['cast'] == str:
                if string == "min":
                    dtype = "ZZZZZZZZZZZ"
                elif string == "sum" or string == "avg":
                    print("ERROR: Aggregation type not supported.")
                    return 1
                else:
                    dtype = ""
            elif TABLES[df].dtypes[column]['cast'] == int:
                if string == "min":
                    dtype = 999999999999
                else:
                    dtype = 0
            elif TABLES[df].dtypes[column]['cast'] == float:
                if string == "min":
                    dtype = sys.float_info.max
                else:
                    dtype = 0.0
    return dtype

def get_df_col_and_where_list(cmd):
    tokens = cmd.split()
    join = False
    where = False
    cols_dfs_where = [""]
    for tkn in tokens[1:]:
        if tkn.lower() == "from":
            cols_dfs_where.append("")
        elif tkn.lower() == "join":
            cols_dfs_where.append("")
            join = True
        elif tkn.lower() == "where":
            cols_dfs_where.append("")
            where = True
        else:
            cols_dfs_where[-1] += tkn+" "
    col_list = [v.strip() for v in cols_dfs_where[0].split(",")]
    dfs_list = [v.strip() for v in cols_dfs_where[1].split(",")]
    if join == True:
        join_list = [cols_dfs_where[2].strip()]
        join_list = [v.strip() for v in cols_dfs_where[2].split(" ")]
    else:
        join_list = []
    if where == True and join == True:
        where_list = [cols_dfs_where[3].strip()]
    elif where == True and join == False:
        where_list = [cols_dfs_where[2].strip()]
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
            if agg not in ["min","max","sum","avg"]:
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
        elif "*" in col:
            for df in df_aliases:
                which_columns[df_aliases[df]] = "*"
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
                            df_col = [list(df_aliases.keys())[0],v]
                        
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
                        df_col = [list(df_aliases.keys())[0],col_list[0]]
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
                        df_col = [list(df_aliases.keys())[0],col_string[0]]
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

        if df not in cond_list:
            cond_list[df] = {}

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

def which_join(df1, df2, data1, data2, col1, col2, conjunctive):
    #This function determines using cost-based optimization whether we should call merge_scan or nested_loop
    if len(data1) == 0 or len(data2) == 0:
        return {df1:[],df2:[]}
    merge_cost = len(data1) * math.log(len(data1), 2) + len(data2) * math.log(len(data2), 2) + len(data1) + len(data2)
    nested_cost = len(data1) * len(data2)
    if merge_cost < nested_cost:
        lol = merge_scan(df1, df2, data1, data2, col1, col2, conjunctive)
        return {df1:lol[0],df2:lol[1]}
    if len(data1) < len(data2):
        lol =  nested_loop(df1, df2, data1, data2, col1, col2, conjunctive)
        return {df1:lol[0],df2:lol[1]}
    lol =  nested_loop(df2, df1, data2, data1, col2, col1, conjunctive)
    return {df2:lol[0],df1:lol[1]}

def nested_loop(df1, df2, data1, data2, col1, col2, conjunctive):
    keys1 = []
    keys2 = []
    for i in data1:
        for j in data2:
    #for i in TABLES[df1].table[col1]:
        #for j in TABLES[df2].table[col2]:
                print(i)
                print(j)
                if conjunctive:
                    if col1 == TABLES[df1].key and col2 == TABLES[df2].key:
                        if i == j:
                            keys1.append(i)
                            keys2.append(j)
                    else:
                    #print("in this if")
                    #if i == j:
                    #print(TABLES[df1].table[TABLES[df1].key][i][col1])
                        print(TABLES[df1].table[TABLES[df1].key][i])
                        print(TABLES[df2].table[TABLES[df2].key][j][col2])
                        if TABLES[df1].table[TABLES[df1].key][i][col1] == TABLES[df2].table[TABLES[df2].key][j][col2]:
                            print("in second if")
                            if col1 == TABLES[df1].key:
                                print("in if")
                                keys1.append(i)
                            else:
                                print("in else")
                                temp = TABLES[df1].table[col1]
                                #print(temp)
                                #print(i)
                                #keys1.append(temp[i])
                                keys1.append(i)
                            if col2 == TABLES[df2].key:
                                print("in if2")
                                keys2.append(j)
                            else:
                                print("in else2")
                                #temp = TABLES[df2].table[col2]
                                #keys2.append(temp[i])
                                keys2.append(j)
                else:
                    print("in this else")
                    if col1 == TABLES[df1].key and i not in keys1:
                        keys1.append(data1[i])
                    elif col1 == TABLES[df1].key and i in keys1:
                        pass
                    else:
                        print("in else")
                        temp = TABLES[df1].table[col1]
                        keys1.append(temp[i])
                    if col2 == TABLES[df2].key and j not in keys2:
                        keys2.append(data2[j])
                    elif col2 == TABLES[df2].key and j in keys2:
                        pass
                    else:
                        print("in else")
                        temp = TABLES[df2].table[col2]
                        keys2.append(temp[j])
    return [keys1, keys2]

def merge_scan(df1, df2, data1, data2, col1, col2, conjunctive):
    keys1 = []
    keys2 = []
    i = 0
    j = 0
    data1 = sorted(data1)
    data2 = sorted(data2)
    while i < len(data1) and j < len(data2):
        if data1[i] < data2[j]:
            i = i + 1
        elif data1[i] > data2[j]:
            j = j + 1
        else:
            if conjunctive: 
                if col1 == TABLES[df1].key:
                    keys1.append(data1[i])
                else:
                    temp = TABLES[df1].table[col1]
                    keys1.append(temp[i])
                if col2 == TABLES[df2].key:
                    keys2.append(data2[j])
                else:
                    temp = TABLES[df2].table[col2]
                    keys1.append(temp[i])
            else:
                if col1 == TABLES[df1].key and i not in keys1:
                    keys1.append(data1[i])
                elif col1 == TABLES[df1].key and i in keys1:
                    pass
                else:
                    temp = TABLES[df1].table[col1]
                    keys1.append(temp[i])
                if col2 == TABLES[df2].key and j not in keys2:
                    keys2.append(data2[j])
                elif col2 == TABLES[df2].key and j in keys2:
                    pass
                else:
                    temp = TABLES[df2].table[col2]
                    keys2.append(temp[j])
            i = i + 1
            j = j + 1
    return [keys1, keys2]

def and_optimizer(cond_columns):
    dfs = []
    if cond_columns:
        dfs += list(cond_columns.keys())
    dfs = list(set(dfs))
    for df in dfs:
        if df in cond_columns:
            subset = [cond_columns[df][cond] for cond in cond_columns[df]]
    sorted_cond_columns = sorted(subset, key = len)
    return sorted_cond_columns

def or_optimizer(cond_columns):
    dfs = []
    if cond_columns:
        dfs += list(cond_columns.keys())
    dfs = list(set(dfs))
    subset = []
    for df in dfs:
        if df in cond_columns:
            subset.extend([cond_columns[df][cond] for cond in cond_columns[df]])
    or_keys = []
    for sublist in subset:
        for item in sublist:
            if item not in or_keys:
                or_keys.append(item)
    return or_keys

# endregion OPTIMIZATIONS #####################################################################

def main():

    while True:
        #cmd = get_input()
        #if cmd == ["exit"]:
        #    break
        # cmd = ["CREATE TABLE TeSt (state VARCHAR(15),year INT,emissions_per_cap FLOAT,PRIMARY KEY (state))",
        #        "LOAD DATA LOCAL INFILE 'data/emissions.csv' INTO TABLE emissions FIELDS TERMINATED BY ',' IGNORE 1 ROWS"]
        # cmd = [
        #        "CREATE TABLE df1 (w1 INT, w2 INT, PRIMARY KEY(w1))",
        #        "CREATE TABLE df2 (x1 INT, x2 INT, FOREIGN KEY (x1) REFERENCES df1(w1), PRIMARY KEY(x1))",
        #        "CREATE TABLE df3 (y1 INT, y2 INT, PRIMARY KEY(y1))",
        #        "CREATE TABLE df4 (z1 INT, z2 INT, FOREIGN KEY (z1) REFERENCES df3(y1), PRIMARY KEY(z1))",
        #        "LOAD DATA INFILE 'data/rel_i_i_1000' INTO TABLE df1 IGNORE 1 ROWS",
        #        "LOAD DATA INFILE 'data/rel_i_1_1000' INTO TABLE df2 IGNORE 1 ROWS",
        #        "LOAD DATA INFILE 'data/rel_i_i_10000' INTO TABLE df3 IGNORE 1 ROWS",
        #        "LOAD DATA INFILE 'data/rel_i_1_10000' INTO TABLE df4 IGNORE 1 ROWS",
        #        "INSERT INTO df2 (x1, x2) VALUES (1001,1001))"", # this should throw an error because there is no 1001 in df1, works"
        #        "INSERT INTO df1 (w1, w2) VALUES (1001,1001)",
        #        "INSERT INTO df1 (w1, w2) VALUES (1000,1000)",
        #        "INSERT INTO df2 (x1, x2) VALUES (1001,1001))",
        #         "SELECT * FROM df1 as a",
        #        "UPDATE df1 set w2 = 420 where w2 > 900 AND w2 < 925",
        #        "delete from df1 where w1 == 1000",
        #        "select * from df2",
        #         "SELECT * FROM df3 as a, df2 as b JOIN ON a.y1 = b.x1 WHERE a.y1 < 5 OR b.x1 < 3", # was working but now not working because of parsing issue
        #        "SELECT a.y1, b.x2 FROM df3 as a, df2 as b JOIN ON a.y1 = b.x1 WHERE a.y1 < 5 OR b.x1 < 3",
        #        "SELECT a.y1, b.z1 FROM df3 as a, df4 as b JOIN ON a.y2 = b.z2"
        #       ]
        # # 
        cmd2 = ["create table df1 (Letter varchar(3), Number int, Color VARCHAR(6), primary key (Letter))",
                 "load data infile 'data/df1.csv' into table df1 ignore 1 rows",
                 "create table df2 (decimal float, state varchar(10), year int, name varchar(3), foreign key (name) references df1(Letter), primary key(name))",
                 "insert into df2 (name,decimal,state,year) values (aab,0.2,Minnesota,2002)",
                 "insert into df2 (name,decimal,state,year) values (aao,0.4,Minnesota,2004)", 
                 #"create table df2 (name varchar(3), decimal float, state varchar(10), year int, foreign key (name) references df1(Letter), primary key(name))",
        #         #"load data infile 'data/df2.csv' into table df2 ignore 1 rows",
                 "create table df3 (name varchar(3), color VARCHAR(6), primary key (name))",
                 "insert into df3 (name,color) values (aab,Red)",
                 "insert into df3 (name,color) values (aad,Red)",
                 "insert into df3 (name,color) values (aac,Orange)",
                 "select a.Letter, b.year from df1 a, df2 b join on a.Letter = b.name where a.Number < 15 and a.Number > 5",
                 "select b.name, a.Letter, a.Color FROM df1 a, df3 b JOIN ON a.Color = b.color WHERE a.Color == 'Red'",
         #        ]
        #          "select a.Letter, b.year from df1 a, df2 b join on a.Letter = b.name where a.Number > 99 and a.Letter in ('abt')",
        #         "select a.Letter, b.year from df1 a, df2 b join on a.Letter = b.name where a.Number > 90",
        #         "select a.Letter from df1 a, df2 b join on a.Letter = b.name where b.year == 2004",
        #         "select Letter from df1 where Number > 99",
        #         "select a.Letter, b.name from df1 a, df2 b join on a.Letter = b.name",
        #         "select a.Letter, b.name from df1 a, df2 b join on a.Letter = b.name where a.Number > 50",
        #         "select a.Color, b.name from df1 a, df2 b join on a.Color = b.Color where a.Color in ('Turquo', 'Purple')",
                 "select avg(a.Number) from df1 a where a.Number < 5"]
        #         "select name, decimal, year from df2 where decimal == 0.2 or decimal == 0.6"
        #         ]
        # #cmd = ["create table df1 (Letter varchar(3), Number int, Color VARCHAR(6), primary key (Letter))",
        # #   "load data infile 'data/df1.csv' into table df1 ignore 1 rows",
        # #   "create table df2 (name varchar(3),decimal float, state varchar(10), year int, foreign key (name) references df1(Letter), primary key(name))",
        # #   "load data infile 'data/df2.csv' into table df2 ignore 1 rows",
        # #    "select b.name, b.decimal from df2 as b where b.name not like 'aa%' and b.decimal*2<.05 and b.state <= 'Alabama' and b.decimal*800 + b.year < 1910 and b.state in ('Iowa','Minnesota','Indiana')",
        # #    "select a.Letter, max(a.Number) from df1 as a where a.Letter not like 'aa%' and a.Number*2 < 20 and a.Number + a.Number < 30 and a.Color in ('Orange','Yellow','Blue')",
        # #    "select min(a.Letter) as minimum, b.state from df1 a, df2 as b where a.Letter == b.name and b.decimal in (1,2,3,4)",
        # #    "select a.Letter, b.name from df1 a, df2 b join a.Letter = b.name",
        # #    "select a.Letter, b.name from df1 a, df2 b join a.Letter = b.name where a.Number > 99",
        # #    ]
        process_input(cmd2)

        # #x = nested_loop(TABLES["df1"], TABLES["df3"], "Color", "Color")
        # #output(TABLES["df1"], TABLES["df3"], x)
        # #nested_loop(TABLES["df1"], TABLES["df2"], "Letter", "name")
        # #print(merge_scan(TABLES["df1"], TABLES["df2"], "Letter", "name"))
        # # print(which_join(TABLES["df1"], TABLES["df2"], "Letter", "name"))
        # # print(merge_scan(TABLES["df1"], TABLES["df3"], "Color", "Color"))
        # # cmd2 = ["select test1, test2, test3 from test4, test5 where"]
        # # process_input(cmd2)
        break
        # break
    #TABLES["df2"].sort_table(by="name")
    #TABLES["df2"].print_table()
    #TABLES["df3"].print_table()

    #for name in TABLES:
    #    TABLES[name].sort_table(by="Number")
    #    TABLES[name].print_table()
    #    break

    # print("goodbye")
    return 

if __name__ == "__main__":
    main()