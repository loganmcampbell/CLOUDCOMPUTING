import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner



object assignment8
{
	def main(args: Array[String])
	{
		val conf = new SparkConf().setAppName("assignment8").setMaster("local")
		val sc = new SparkContext(conf)
		val input = sc.textFile("input.txt")
		val iterations = if (args.length > 0) args(0).toInt else 10
		println("ITERATIONS : " + iterations)
		val pairs = input.map
		{
			s =>
			val parts = s.split(" ")
      		(parts(0), parts(1))
    }
			val links = pairs.distinct().groupByKey().cache()
    	var ranks = links.mapValues(v => 1.0)
			var i = 0
		for ( i <- 1 to iterations)
		{

      		val weight = links.join(ranks).values.flatMap
					{ case (connections, rank) =>
        	val size = connections.size
        	connections.map(place => (place, rank / size))
					}

      		ranks = weight.reduceByKey(_ + _).mapValues(0.1 + .9 * _ )
    }

		ranks.saveAsTextFile("output")

	}
}
