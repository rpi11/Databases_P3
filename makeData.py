import random, csv, itertools

letters = [chr(i) for i in range(ord('a'), ord('z')+1)]
keys = itertools.product(letters,letters,letters)
colors = ['Red', 'Green', 'Blue', 'Yellow', 'Orange', 'Purple', 'Pink', 'Cyan', 'Magenta', 'Turquoise', 'Lavender', 'Brown', 'Gray', 'Black', 'White']
states = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
    'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
    'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri',
    'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina',
    'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
    'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
]


with open("data/df1.csv", "w+") as o:
    writer = csv.writer(o)
    writer.writerow(["Letter","Number","Color"])

    for key in keys:
        writer.writerow(["".join(list(key)),random.randint(0,100),random.choice(colors)])

with open("data/df2.csv", "w+") as o:
    writer = csv.writer(o)
    writer.writerow(["name","decimal","state","year"])

    



