variable "name" {
  description = "Name prefix for the network resources, for example changeport-dev."
  type        = string
}

variable "cidr_block" {
  description = "IPv4 CIDR block of the VPC, a /16 to /20; a /16 leaves room for about 49,000 pods."
  type        = string

  validation {
    condition     = can(cidrhost(var.cidr_block, 0)) && tonumber(split("/", var.cidr_block)[1]) >= 16 && tonumber(split("/", var.cidr_block)[1]) <= 20
    error_message = "The VPC needs an IPv4 CIDR block from /16 to /20."
  }
}

variable "availability_zones" {
  description = "The three availability zones the subnets span, by zone ID (such as use1-az1), which names the same zone in every account."
  type        = list(string)

  validation {
    condition     = length(var.availability_zones) == 3
    error_message = "Kafka clusters are spread across exactly three availability zones."
  }
}

variable "nat_gateway_per_zone" {
  description = "Whether each availability zone gets its own NAT gateway, so the loss of a zone cannot cut the others off from the internet (prod), or all share one (dev, cheaper)."
  type        = bool
}
