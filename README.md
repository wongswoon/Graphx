# Graphx
use spark graphx find 2 hops neighbors
For most social relationships, it's far from enough to get one hop, and another important indicator is the two hop neighborhood. For example, no secret App friends of the friends of the secret, a wider range of communication, the amount of information is more abundant. Therefore, the number of two hop neighbors of the statistics is a map of the physical examination is a very important indicator. For the two hop neighbor calculation, GraphX does not appear to be the interface, the need for their own design and development. 
## problem
created a directed graph, using graphx.

#src->dest weight
a  -> b  34
a  -> c  23
b  -> e  10
c  -> d  12
d  -> c  12

we got result like:
a->e via b weight=34+10=44
a->d via c weight=23+12=35 and  so on...

imaging this graph is a huge natural graph,which means the graph has many edges 10billion+ and each node has diffent degree from 0 to 20000

## how to solve it?
Currently used method is: the first traversal, all point to neighbor point spread a with its own ID, the value of life for 2 of the message; the second traversal, all will receive the message to the neighbors and then forwarded a, life value is 1. The final statistics for all points on, received the life value ID for 1, and to group the summary, get all the points of the second hop neighbors.
1. partition
it's very important to avoid data skew
see https://issues.apache.org/jira/browse/SPARK-3523
we choose HybridCut strategy for edge partiton 
```
val edgeRdd = sc.textFile(epath)
      .map { line =>
      val fields = line.split(sp)
      Edge(fields(0).toLong, fields(1).toLong, (fields(2).toInt)
    }.map { 
      //
      e => ( ((math.abs(dstId) * mixingPrime) % numParts).toInt, e) }.
      partitionBy(new HashPartitioner(numParts)).mapPartitions { iter =>
      val messages = iter.toArray
      val indegrees = new Long2IntOpenHashMap()
      messages.foreach { message =>
        val value = indegrees.get(message._2.dstId)
        if (value != 0)
          indegrees.put(message._2.dstId, value + 1)
        else
          indegrees.put(message._2.dstId, 1)
      }
      messages.map { message =>
        if (indegrees(message._2.dstId) <= threshold) {
          message
        } else {
          (((math.abs(dstId) * mixingPrime) % numParts).toInt, message._2)
        }
      }.toIterator
    }.partitionBy(new HashPartitioner(numParts)).map {
      _._2
    }
```
2. save the memory
use google protobuf and fastutils map
we put neb in vertex props map, the map support by fastutils#Long2ObjectOpenHashMap, key is vertexid, value is the nebrs info that compress to bytes array with protobuf
