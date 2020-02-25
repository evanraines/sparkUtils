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
def returnKeyVal[T](x: Array[Map[T, Int]], k: T): Int = {
    x.flatten.filter(_._1 == k).map(_._2).sum
}

/*
def returnKeySet(maps):
    """this returns the keys for each map"""
    return list(set([key for m in maps for key in m if key != '']))
*/
def returnKeySet[T](x: Array[Map[T, Any]]): Array[T] = {
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

def reduceMaps(row: Array[Map[String, String]]): Map[String, String] = {
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

/* bunch of udfs expressed as lambdas in python
modalUdf = udf(lambda map: sum([int(float(map[x])) if "modal" in x else 0 for x in map]), IntegerType())                        # loop through map to find keyword and then sum list
offeredUdf = udf(lambda map: sum([int(float(map[x])) if "subscriptions_offered" in x else 0 for x in map]), IntegerType())        # loop through map to find keyword and then sum list
selectedUdf = udf(lambda map: sum([int(float(map[x])) if "subscription_selected" in x else 0 for x in map]), IntegerType())        # loop through map to find keyword and then sum list
conversionUdf = udf(lambda map: sum([int(float(map[x])) if "conversion" in x else 0 for x in map]), IntegerType())                   # loop through map to find keyword and then sum list
lowerUdf = udf(lambda arr: [x.lower() for x in arr], ArrayType(StringType()))
*/

def sumElementsUdf(m: Map[], ele: String): Int = m.filter(_._1.contains(ele)).map(_._2.toInt).sum 
def lowerUdf(a: Array[String]): Array[String] = a.map(_.toLower)
}
