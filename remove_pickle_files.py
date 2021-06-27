import os

os.chdir("results")
for filename in os.listdir("."):
    print(filename)
    os.remove(filename)
