# Graphx
use spark graphx find 2 hops neighbors
For most social relationships, it's far from enough to get one hop, and another important indicator is the two hop neighborhood. For example, no secret App friends of the friends of the secret, a wider range of communication, the amount of information is more abundant. Therefore, the number of two hop neighbors of the statistics is a map of the physical examination is a very important indicator. For the two hop neighbor calculation, GraphX does not appear to be the interface, the need for their own design and development. 
## problem
created a directed graph, using graphx.
e.g.

src|dest |weight
--------------
a  | b  34
a  | c  23
b  | e  10
c  | d  12
d  | c  12

we got result like:

a->e via b weight=34+10=44

a->d via c weight=23+12=35 and so on...

Imaging that the graph is a huge natural graph like SNS network, which means it has many edges (10billion+) and each node has diffent degree from 0 to 20000

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
Using google protobuf and fastutils map

we put nebrs in vertex props which is a map support by `fastutils#Long2ObjectOpenHashMap` and the key is vertexid, value is the nebrs info that compressed to bytes array with protobuf

3. find the nebrs
the code below show how to find 1 hop nebrs
``` type LMap[V] = Long2ObjectOpenHashMap[V]
    type LBMap = Long2ObjectOpenHashMap[Array[Byte]]
    val vertices1 = g.aggregateMessages[LBMap](
      edgeCtx => {
        if (edgeCtx.dstAttr != null) {
          val itr = edgeCtx.dstAttr.entrySet().iterator()
          while (itr.hasNext) {
            val entry = itr.next()
            val rv = PBUtil.parseFromPB(entry.getValue)
            if (rv.getDist == 0) {
              val dist = rv.getDist
              val brs = rv.getBridges
              rv.setDist(dist + 1)
              rv.setScore(edgeCtx.attr._2)
              brs.get(0).setBri_score(edgeCtx.attr._2)
              brs.get(0).setBri_id(edgeCtx.srcId)
              // expected size is 1
              val msg = new LMap[Array[Byte]](1)
              msg.put(entry.getKey.longValue(), PBUtil.toPBObject(rv))
              edgeCtx.sendToSrc(msg)
            }
          }
        }
      },
      (a, b) => {
        a.putAll(b)
        a
      },
      TripletFields.Dst
    )
```
4. reuse the origin graph
When we have got the 1 hop nebrs which repersenting by VertexRDD `vertices1`,
now we want to find 2 hop nebrs, we should reuse the origin graph, by using `joinVertices` rather not 
new Graph(vertices1, g.edges)
to reasign the vetices value to the graph.Benifiting by that, we can save memory for allocating the new graph.
```
 g = g.joinVertices(vertices1) {
      (vid, old, newAttr) => newAttr
    }
 
 ```
