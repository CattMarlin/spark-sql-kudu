package example

import net.jcazevedo.moultingyaml._

 /**
  * Custom YAML Protocol
  */
object YamlProtocol extends DefaultYamlProtocol {
  implicit val query: YamlFormat[Query] = yamlFormat3(Query)
  implicit val allQueries: YamlFormat[AllQueries] = yamlFormat1(AllQueries)
}

/**
 * Methods to parse an table yaml into case classes
 */

object YamlConfigParser {

  def convert(yaml: String): AllQueries = {
    import YamlProtocol._
    yaml.parseYaml.convertTo[AllQueries]
  }
}

/**
  * Case Classes for Custom YAML Protocol
  */
case class AllQueries(all_queries: List[Query])

case class Query(name: String,
                 query: String,
                 kudu_tables: List[String])

