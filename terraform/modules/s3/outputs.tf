output "tiered_storage_bucket" {
  description = "Name of the tiered storage bucket."
  value       = aws_s3_bucket.tiered_storage.bucket
}

output "tiered_storage_bucket_arn" {
  description = "ARN of the tiered storage bucket."
  value       = aws_s3_bucket.tiered_storage.arn
}
