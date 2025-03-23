terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "1.7.0" # or the latest version
    }
  }
}

provider "confluent" {
  kafka_api_key     = var.confluent_api_key
  kafka_api_secret  = var.confluent_api_secret
  kafka_rest_endpoint = var.confluent_rest_endpoint
}

resource "confluent_kafka_topic" "example_topic" {
  kafka_cluster {
    id = var.confluent_kafka_cluster_id
  }

  topic_name       = var.topic_name
  partitions_count = 3

  config = {
    "retention.ms"       = "604800000"
    "cleanup.policy"     = "delete"
    "min.insync.replicas" = "2"
  }
}
