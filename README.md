TECHNOLOGIES: 
- Scala 2.11.8
- Spark 2.1.0
- sbt

Reasons:
- Eye towards Scalability
- Satisfying functional programming requirement, for free
- Reasonable familiarity with tech stack

ASSUMPTIONS:

If two uids are:
  1. on the same floor,
  2. within 5 minutes of each other, 
  3. within 3 metres of each other,
then a meeting is said to have occurred between them.

USAGE: 
- Change into working directory
- Unzip the input data file into a new directory, 'data' within the home directory as: <home_dir>/data/reduced.csv
- sbt assembly
- /usr/local/Cellar/apache-spark/2.1.0/bin/spark-submit --class com.challenges.elastictry.MeetingDecisionEngine --master local[4] target/scala-2.11/meetdecisionengine-v1.jar <uid1> <uid2>

PERFORMANCE:
- The algorithm has time complexity of O(n logn) on average, or O(n^2) in the worst-case
- The algorithm has space complexity of O(n + n), that asympotitically is O(n) in both average and the worst-case

EXTENSIBILITY:
- To support large batches of queries, or queries via a web-service it would make sense to have a wrapper over this module, passing in two uids at a time, and persisting the output.

- To support an infinite stream of input, I'd propose using Kafka, due to my familiarity with it, or use Spark Streaming. Once again, it is a matter of writing extensible wrappers while keeping this module as-is.