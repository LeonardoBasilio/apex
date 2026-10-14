variable "region" {
  description = "AWS region for the whole stack."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for every resource name/tag in this stack."
  type        = string
  default     = "apex-portfolio"
}

variable "vpc_cidr" {
  description = "CIDR for the dedicated VPC this stack creates. Not the default VPC -- see README.md for why."
  type        = string
  default     = "10.42.0.0/16"
}

variable "azs" {
  description = "Two availability zones. MSK's smallest viable cluster needs brokers in >= 2 AZs (number_of_broker_nodes must be a multiple of the subnet count)."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "msk_broker_instance_type" {
  description = "Smallest MSK broker type suitable for a demo/dev workload -- not sized for production throughput."
  type        = string
  default     = "kafka.t3.small"
}

variable "msk_kafka_version" {
  description = "Must stay compatible with store-postgres/'s KRaft-mode local stack's broker protocol expectations for consumers written against kafka-python."
  type        = string
  default     = "3.5.1"
}

variable "rds_instance_class" {
  description = "Smallest Postgres instance class suitable for a demo/dev workload."
  type        = string
  default     = "db.t3.micro"
}

variable "rds_allocated_storage_gb" {
  type    = number
  default = 20
}

variable "rds_engine_version" {
  type    = string
  default = "16.4"
}

variable "db_name" {
  type    = string
  default = "apex"
}

variable "db_username" {
  type    = string
  default = "apex"
}

variable "db_password" {
  description = "No default on purpose -- pass via TF_VAR_db_password or a .tfvars file that is gitignored, never committed. See README.md."
  type        = string
  sensitive   = true
}

variable "allowed_cidr_blocks" {
  description = <<-EOT
    CIDRs allowed to reach MSK (9092) and Postgres (5432). A CIDR allowlist is
    a placeholder for this portfolio artifact -- a real deployment should
    reference the consuming compute's own security group (the
    store-postgres/ consumer's ECS task SG, or a Databricks workspace's VPC
    endpoint SG), not an open CIDR range. See README.md's "What this is NOT"
    section.
  EOT
  type        = list(string)
  default     = []
}
