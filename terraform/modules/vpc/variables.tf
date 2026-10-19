variable "name" {
  description = "Name prefix for the network resources, for example changeport-dev."
  type        = string
}

variable "cidr_block" {
  description = "IPv4 CIDR block of the VPC."
  type        = string
}

variable "availability_zones" {
  description = "The three availability zones the subnets span."
  type        = list(string)

  validation {
    condition     = length(var.availability_zones) == 3
    error_message = "Kafka clusters are spread across exactly three availability zones."
  }
}
