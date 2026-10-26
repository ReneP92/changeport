output "registry" {
  description = "The registry's host name, such as 123456789012.dkr.ecr.us-east-1.amazonaws.com."
  value       = split("/", values(aws_ecr_repository.image)[0].repository_url)[0]
}

output "push_role_arn" {
  description = "The role the image workflow assumes, the ECR_PUSH_ROLE_ARN repository variable on GitHub."
  value       = aws_iam_role.push.arn
}
