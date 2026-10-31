# Plans the module against a mocked AWS provider, so the subnet layout, the
# tags EKS Auto Mode and the per-customer NodeClasses depend on, the NAT
# gateways and the S3 endpoint are checked without an AWS account.

mock_provider "aws" {
  mock_data "aws_region" {
    defaults = {
      region = "us-east-1"
    }
  }
}

variables {
  name                 = "changeport-test"
  cidr_block           = "10.10.0.0/16"
  availability_zones   = ["use1-az1", "use1-az2", "use1-az4"]
  nat_gateway_per_zone = false
}

run "subnets" {
  command = plan

  assert {
    condition     = module.vpc.private_subnets_cidr_blocks == tolist(["10.10.0.0/18", "10.10.64.0/18", "10.10.128.0/18"])
    error_message = "The private subnets must be the first three quarters of the VPC, one per zone."
  }
  assert {
    condition     = module.vpc.public_subnets_cidr_blocks == tolist(["10.10.192.0/24", "10.10.193.0/24", "10.10.194.0/24"])
    error_message = "The public subnets must be /24s at the start of the last quarter, one per zone."
  }
  assert {
    condition = alltrue([for subnet in module.vpc.private_subnet_objects :
      subnet.tags["kubernetes.io/role/internal-elb"] == "1" && subnet.tags["changeport.io/customer-nodes"] == "true"
    ])
    error_message = "Private subnets need the internal load balancer and customer node tags."
  }
  assert {
    condition = alltrue([for subnet in module.vpc.public_subnet_objects :
      subnet.tags["kubernetes.io/role/elb"] == "1" && !contains(keys(subnet.tags), "changeport.io/customer-nodes")
    ])
    error_message = "Public subnets need the load balancer tag, and must not take customer nodes."
  }
  assert {
    condition     = tolist([for subnet in module.vpc.private_subnet_objects : subnet.availability_zone_id]) == tolist(["use1-az1", "use1-az2", "use1-az4"])
    error_message = "The subnets must be placed by zone ID."
  }
}

run "one_nat_gateway" {
  command = plan

  assert {
    condition     = length(module.vpc.natgw_ids) == 1
    error_message = "Without a NAT gateway per zone, the VPC has one."
  }
  assert {
    condition     = length(output.nat_public_ips) == 1
    error_message = "The one NAT gateway's address is where customers' databases see the platform connect from."
  }
}

run "nat_gateway_per_zone" {
  command = plan

  variables {
    nat_gateway_per_zone = true
  }

  assert {
    condition     = length(module.vpc.natgw_ids) == 3
    error_message = "With a NAT gateway per zone, the VPC has three."
  }
  assert {
    condition     = length(module.vpc.private_route_table_ids) == 3
    error_message = "Each zone's private subnet routes through its own NAT gateway."
  }
  assert {
    condition     = length(output.nat_public_ips) == 3
    error_message = "Customers' databases allowlist the addresses of all three NAT gateways."
  }
}

run "s3_endpoint" {
  command = plan

  assert {
    condition     = aws_vpc_endpoint.s3.service_name == "com.amazonaws.us-east-1.s3" && aws_vpc_endpoint.s3.vpc_endpoint_type == "Gateway"
    error_message = "Tiered storage needs an S3 gateway endpoint in the VPC's region."
  }
}

run "two_zones_are_refused" {
  command = plan

  variables {
    availability_zones = ["use1-az1", "use1-az2"]
  }

  expect_failures = [var.availability_zones]
}

run "small_vpcs_are_refused" {
  command = plan

  variables {
    cidr_block = "10.10.0.0/24"
  }

  expect_failures = [var.cidr_block]
}
