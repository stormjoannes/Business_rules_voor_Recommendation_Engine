name = "L'Oréal Elnett Satin Extra Sterke Fixatie Haarspray 75 ml"
if "'" in name:
    name = name.split("'")
    name = name[0] + "''" + name[1]
print(name)