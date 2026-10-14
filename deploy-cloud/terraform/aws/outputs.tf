output "msk_bootstrap_brokers" {
  description = "Feed to KAFKA_BOOTSTRAP_SERVERS for stream-kafka/'s producer and store-postgres/'s consumer."
  value       = aws_msk_cluster.this.bootstrap_brokers
}

output "rds_endpoint" {
  description = "host:port for POSTGRES_DSN."
  value       = aws_db_instance.this.endpoint
}

output "rds_db_name" {
  value = aws_db_instance.this.db_name
}

output "vpc_id" {
  value = aws_vpc.this.id
}

output "subnet_ids" {
  value = aws_subnet.this[*].id
}
