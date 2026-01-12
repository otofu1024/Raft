other_nodes = "n2:8000,n3:8000".split(",")
other_nodes = list(map(lambda x: x.split(":"), other_nodes))
print(other_nodes)