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
def returnKeyVal(x: Array[Map[String, Int]], k: String): Int = {
    x.flatten.filter(_._1 == k).map(_._2).sum
}

/*
def returnKeySet(maps):
    """this returns the keys for each map"""
    return list(set([key for m in maps for key in m if key != '']))
*/
def returnKeySet(x: Array[Map[String, Any]]): Array[String] = {
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

def sumElementsUdf(m: Map[String, String], ele: String): Int = m.filter(_._1.contains(ele)).map(_._2.toInt).sum 
def lowerUdf(a: Array[String]): Array[String] = a.map(_.toLowerCase)

/*
def ranked(map, b):
                for key in map:
                    try:
                        map[key] = int(float(map[key]))
                    except:
                        map[key] = 0
                pb, ppv, sb, spv, tb, tpv = None, None, None, None, None, None
                for w in sorted(map, key=map.get, reverse=True):
                    tb = w if tb is None and pb is not None and sb is not None and map[w] != 0 else tb
                    tpv = int(map[w]) if tpv is None and ppv is not None and spv is not None and map[w] != 0 else tpv
                    sb = w if sb is None and pb is not None and map[w] != 0 else sb
                    spv = int(map[w]) if spv is None and ppv is not None and map[w] != 0 else spv
                    pb = w if pb is None and map[w] != 0 else pb
                    ppv = int(map[w]) if ppv is None and map[w] !=0 else ppv        
                return ((pb, ppv), (sb, spv), (tb, tpv))
*/

def rankedUDF(m: Map[String, String]) = {
    // value to int and get top 3 instances
    val ordered_m: Seq[(String, Int)] = {
        m
        .map {x => (x._1, x._2.toInt)}
        .toSeq
        .sortWith {_._2 >= _._2}
        .slice(0,3)
    }
    
    def inner(m: Seq[(String, Int)], acc: Seq[(String, Int)]): Seq[(String, Int)] = {
        m match {
            case Nil => acc
            case _ => inner(m.tail, acc :+ m.head)
        }
    }

    inner(ordered_m, Seq.empty[(String, Int)])
}

}
