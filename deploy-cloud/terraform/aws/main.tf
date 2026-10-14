# Apex (portfolio port) -- AWS target for the capture -> stream -> store
# chain this branch already runs locally via Docker Compose
# (store-postgres/docker-compose.yml). This maps the SAME two stateful
# services -- Kafka and Postgres -- onto managed AWS equivalents (MSK, RDS),
# in a dedicated VPC rather than the account's default one, so this stack's
# blast radius is visible and removable as one unit (`terraform destroy`
# touches nothing outside it).
#
# NOT APPLIED. Written and reviewed for HCL correctness
# (`terraform validate` / `terraform fmt -check`), never run against a real
# AWS account from this session -- see README.md's "What this is NOT"
# section for exactly what that means and doesn't mean.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

locals {
  tags = {
    Project   = "apex-portfolio-port"
    ManagedBy = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Networking -- a dedicated VPC, not the account default. Two public-ish
# subnets (no NAT; this demo has no outbound-internet-needing compute in it)
# across two AZs, which is the minimum MSK's smallest broker count needs.
# ---------------------------------------------------------------------------

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = merge(local.tags, { Name = "${var.name_prefix}-vpc" })
}

resource "aws_subnet" "this" {
  count             = length(var.azs)
  vpc_id            = aws_vpc.this.id
  availability_zone = var.azs[count.index]
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, count.index)
  tags              = merge(local.tags, { Name = "${var.name_prefix}-subnet-${count.index}" })
}

# ---------------------------------------------------------------------------
# Security groups. `allowed_cidr_blocks` is a placeholder -- see
# variables.tf's description and README.md's "What this is NOT" section.
# ---------------------------------------------------------------------------

resource "aws_security_group" "msk" {
  name        = "${var.name_prefix}-msk"
  description = "Apex portfolio port: MSK broker access"
  vpc_id      = aws_vpc.this.id
  tags        = local.tags

  ingress {
    description = "Kafka plaintext broker port, from allowed_cidr_blocks only"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "rds" {
  name        = "${var.name_prefix}-rds"
  description = "Apex portfolio port: Postgres access"
  vpc_id      = aws_vpc.this.id
  tags        = local.tags

  ingress {
    description = "Postgres, from allowed_cidr_blocks only"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------------------------------------------------------------------------
# MSK -- replaces store-postgres/docker-compose.yml's single-node KRaft
# `kafka` service. Plaintext client-broker encryption to match this port's
# stream-kafka/ producer and store-postgres/ consumer as written today
# (neither configures TLS) -- a real deployment should turn this on and
# update both lanes' client config together; noted, not silently assumed
# away.
# ---------------------------------------------------------------------------

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.name_prefix}-kafka"
  kafka_version          = var.msk_kafka_version
  number_of_broker_nodes = length(aws_subnet.this)
  tags                   = local.tags

  broker_node_group_info {
    instance_type   = var.msk_broker_instance_type
    client_subnets  = aws_subnet.this[*].id
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT"
      in_cluster    = true
    }
  }
}

# ---------------------------------------------------------------------------
# RDS Postgres -- replaces the `postgres` service. `store-postgres/`'s own
# DDL (ddl/001_stage_events.sql) still owns schema application; this
# resource only provisions the empty database, same division of
# responsibility as the local Compose stack.
# ---------------------------------------------------------------------------

resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-db-subnets"
  subnet_ids = aws_subnet.this[*].id
  tags       = local.tags
}

resource "aws_db_instance" "this" {
  identifier             = "${var.name_prefix}-postgres"
  engine                 = "postgres"
  engine_version         = var.rds_engine_version
  instance_class         = var.rds_instance_class
  allocated_storage      = var.rds_allocated_storage_gb
  db_name                = var.db_name
  username               = var.db_username
  password               = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  skip_final_snapshot    = true
  deletion_protection    = false
  tags                   = local.tags
}
