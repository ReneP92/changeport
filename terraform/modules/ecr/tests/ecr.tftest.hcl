# Applies the module to a mocked AWS provider and checks the repositories, who
# may pull from them and who may push, without an AWS account.

mock_provider "aws" {
  mock_data "aws_organizations_organization" {
    defaults = {
      id = "o-abcdefghij"
    }
  }
  mock_resource "aws_iam_openid_connect_provider" {
    defaults = {
      arn = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
    }
  }
  mock_resource "aws_ecr_repository" {
    defaults = {
      arn            = "arn:aws:ecr:us-east-1:123456789012:repository/changeport/mocked"
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/changeport/mocked"
    }
  }
}

variables {
  name              = "changeport-test"
  github_repository = "ReneP92/changeport"
}

run "repositories" {
  command = apply

  assert {
    condition     = toset([for repository in aws_ecr_repository.image : repository.name]) == toset(["changeport/kafka", "changeport/agent", "changeport/provisioner", "changeport/console"])
    error_message = "Every image the platform builds gets a repository."
  }
  assert {
    condition = alltrue([for repository in aws_ecr_repository.image :
      repository.image_tag_mutability == "IMMUTABLE" && one(repository.image_scanning_configuration).scan_on_push
    ])
    error_message = "Tags are immutable and images are scanned on push."
  }
  assert {
    condition = alltrue([for policy in aws_ecr_lifecycle_policy.image :
      jsondecode(policy.policy).rules == [{
        rulePriority = 1
        description  = "Keep the newest 200 tagged images"
        selection    = { tagStatus = "tagged", tagPatternList = ["*"], countType = "imageCountMoreThan", countNumber = 200 }
        action       = { type = "expire" }
      }]
    ])
    error_message = "Only tagged images beyond the newest 200 expire."
  }
  assert {
    condition     = output.registry == "123456789012.dkr.ecr.us-east-1.amazonaws.com"
    error_message = "The registry is the host part of the repository URLs."
  }
}

run "the_organization_pulls" {
  command = apply

  assert {
    condition = alltrue([for policy in aws_ecr_repository_policy.image :
      jsondecode(policy.policy).Statement == [{
        Sid       = "PullFromTheOrganization"
        Effect    = "Allow"
        Principal = "*"
        Action    = ["ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"]
        Condition = { StringEquals = { "aws:PrincipalOrgID" = "o-abcdefghij" } }
      }]
    ])
    error_message = "Any principal of the organization may pull, and nobody else, and nobody may push through the repository policy."
  }
}

run "only_main_pushes" {
  command = apply

  assert {
    condition     = jsondecode(aws_iam_role.push.assume_role_policy).Statement[0].Condition.StringEquals["token.actions.githubusercontent.com:sub"] == "repo:ReneP92/changeport:ref:refs/heads/main"
    error_message = "Only workflow runs for pushes to main may assume the push role."
  }
  assert {
    condition     = jsondecode(aws_iam_role.push.assume_role_policy).Statement[0].Condition.StringEquals["token.actions.githubusercontent.com:aud"] == "sts.amazonaws.com"
    error_message = "The token must be meant for AWS STS."
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.push.policy).Statement[1].Resource == [for repository in aws_ecr_repository.image : repository.arn]
    error_message = "The push role may push to the platform's repositories only."
  }
}

run "bad_repository_names_are_refused" {
  command = plan

  variables {
    github_repository = "changeport"
  }

  expect_failures = [var.github_repository]
}
