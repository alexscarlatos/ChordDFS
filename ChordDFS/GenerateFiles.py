import sys

if __name__ == "__main__":
    nodeName = sys.argv[1]
    numFiles = int(sys.argv[2])

    altRate = 5

    for i in range(numFiles):
        with open("nodes/{0}/files/client/{1}.txt".format(nodeName, i), "w") as f:
            f.write("This is file number {0}... Yay!\n".format(i))

    with open("clientScript.txt", "w") as scriptFile:
        for x in range(0, numFiles, altRate):
            for i in range(x, x + altRate):
                scriptFile.write("insert {0}.txt\n".format(i))
            for i in range(x, x + altRate):
                scriptFile.write("get {0}.txt\n".format(i))
        scriptFile.write("entries\n")
