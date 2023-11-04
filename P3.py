import csv, json

TABLES = {}
class Table:
    def __init__(self, col_dtype_dict, key = tuple()):

        return

def create_table(cmd):
    cmd = " ".join(cmd)

    i = 0
    while cmd[i] != "(":
        print(cmd[i])
        i += 1
    
    name = cmd[:i].strip()
    cols = cmd[i+1:-1].strip().split(",")

    columns = {}
    primary_key = ""
    for col in cols:
        if "primary key" in col.lower():
            primary_key = col.split()[-1][1:-1]
        else:
            c = col.split()[0]
            d = col.split()[1].replace("("," ").replace(")","").split()

            

            print(c, d)




    print(name)
    print(cols)


def process_input(cmd_list):
    for cmd in cmd_list:
        tokens = cmd.split()

        if [t.lower() for t in tokens[:2]] == ["create","table"]:
            create_table(tokens[2:])

        # Parse and handle appropriately

def get_input():
    command = ""
    while ";" not in command:
        command += " "+input("> ")
    return command.split(";")

def main():

    while True:
        # cmd = get_input()
        # if cmd == "exit;":
        #     break
        cmd = ["create table comapny_info( name varchar(40),revenue_22_23_e9 varchar(100),primary key (name))"]
        process_input(cmd)
        break

    # print("goodbye")
    return 

if __name__ == "__main__":
    main()