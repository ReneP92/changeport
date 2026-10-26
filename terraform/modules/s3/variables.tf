variable "name" {
  description = "Name prefix of the bucket, for example changeport-dev, which makes changeport-dev-tiered-storage as eksTieredStorage in internal/provisioning/spec.go expects."
  type        = string
}

variable "versioning" {
  description = "Whether the bucket keeps old versions of what is deleted or overwritten (prod), for noncurrent_version_expiration_days."
  type        = bool
}

variable "noncurrent_version_expiration_days" {
  description = "Days an old version is kept when versioning is on."
  type        = number
  default     = 7

  validation {
    condition     = var.noncurrent_version_expiration_days >= 1
    error_message = "Old versions are kept for at least a day."
  }
}
