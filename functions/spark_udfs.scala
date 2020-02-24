/*
converting some old python udfs to scala
*/

object sparkUDFs {
/* RETURN KEY VALUE (python implementation)
def returnKeyVal(maps, key):
    """this returns the _SUM_ of values associated with each key"""
    val = 0
    for m in maps:
        for k in m:
            if k == key:
                val += int(m[k])
    return val
*/
def returnKeyVal[T](x: List[Map[T, Int]], k: T): Int = {
    x.flatten.filter(_._1 == k).map(_._2).sum
}

/*
def returnKeySet(maps):
    """this returns the keys for each map"""
    return list(set([key for m in maps for key in m if key != '']))
*/
def returnKeySet[T](x: List[Map[T, Any]]): List[T] = {
    x.flatMap(x => x.map(_._1)).distinct
}

/* COMBINE MAP (python impolimentation)
""" This takes an array of maps and makes them unique AND sums their counts """
combineMap = udf(lambda maps: dict(zip(
        returnKeySet(maps),
        [returnKeyVal(maps, f) for f in returnKeySet(maps)]
    )),
        MapType(StringType(), StringType())
    )
*/

def reduceMaps(row: Seq[Map[String, String]]): Map[String, String] = {
    row.flatten.groupBy(_._1).map(x => (x._1, x._2.map(_._2.toInt).sum.toString))
}

/* reuturn a string of actions sorted in ascending order of value of map
def actionstring(map):
                actionlist = []
                for key in sorted(map, key=map.get, reverse=False):
                    actionlist.append(key)
                s = "|"
                actions = s.join(actionlist)
                return (actions)
*/
def reduceMapConcatString(row: Map[String, String], c: String): String = {
    val s = row.toSeq.sortWith(_._2<=_._2).map(_._1)
    s.mkString(c)
}
}
