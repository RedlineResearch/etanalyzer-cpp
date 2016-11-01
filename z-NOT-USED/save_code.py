
def summarize_stable_grouplist( sumSTABLE = {},
                                stable_grouplist = [],
                                dgroup_reader = {},
                                objreader = {} ):
    # Summary is indexed by stable group number
    for index in xrange(len(stable_grouplist)):
        graph = stable_grouplist[index]
        sumSTABLE[index][""] = set()
        sumSTABLE[index]["objects"] = set()
        # Create an alias for "objects"
        objset = sumSTABLE[index]["objects"]
        for node in graph.nodes():
            objId = int(node)
            objset.add(objId)

