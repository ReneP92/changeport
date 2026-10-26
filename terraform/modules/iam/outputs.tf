output "kafka_broker_role_name" {
  description = "Name of the brokers' role, the RoleName of eksTieredStorage in internal/provisioning/spec.go."
  value       = aws_iam_role.kafka_broker.name
}

output "kafka_broker_policy" {
  description = "The brokers' permissions, for checking them with the IAM policy simulator."
  value       = jsonencode(local.kafka_broker_policy)
}
