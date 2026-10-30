# The container registry of the platform's own images, in the prod account:
# the Kafka broker image (images/kafka), Kafka Connect (images/connect), the
# Kafka agent, the registry gateway, the provisioner and the console. Every account in the organization pulls from it, and only the
# image workflow on main (.github/workflows/publish-images.yml) pushes, by
# commit tag; the EKS platforms use the images by digest.

data "aws_organizations_organization" "current" {}

resource "aws_ecr_repository" "image" {
  for_each = toset(var.repositories)

  name = "changeport/${each.key}"
  # A tag always names the same image, so a commit tag means what it says.
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# The workflow only pushes an image when its sources change, so the newest
# images cover a long history. The platforms pin a digest from the newest
# publish, which this keeps, and an image older than the newest
# keep_tagged_images is expired. Untagged manifests, which include the
# per-architecture images of each multi-architecture image, are not touched.
resource "aws_ecr_lifecycle_policy" "image" {
  for_each = aws_ecr_repository.image

  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the newest ${var.keep_tagged_images} tagged images"
      selection = {
        tagStatus      = "tagged"
        tagPatternList = ["*"]
        countType      = "imageCountMoreThan"
        countNumber    = var.keep_tagged_images
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_ecr_repository_policy" "image" {
  for_each = aws_ecr_repository.image

  repository = each.value.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "PullFromTheOrganization"
      Effect    = "Allow"
      Principal = "*"
      Action    = local.pull_actions
      Condition = {
        StringEquals = { "aws:PrincipalOrgID" = data.aws_organizations_organization.current.id }
      }
    }]
  })
}

locals {
  pull_actions = [
    "ecr:BatchCheckLayerAvailability",
    "ecr:BatchGetImage",
    "ecr:GetDownloadUrlForLayer",
  ]
  push_actions = [
    "ecr:BatchCheckLayerAvailability",
    "ecr:BatchGetImage",
    "ecr:CompleteLayerUpload",
    "ecr:DescribeImages",
    "ecr:InitiateLayerUpload",
    "ecr:PutImage",
    "ecr:UploadLayerPart",
  ]
}

# GitHub Actions authenticates with OpenID Connect, so no access key exists.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

resource "aws_iam_role" "push" {
  name        = "${var.name}-image-push"
  description = "Pushes the platform's images from GitHub Actions on main"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          # Workflow runs for pushes to main only: not pull requests, other
          # branches, tags or GitHub environments.
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repository}:ref:refs/heads/main"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "push" {
  name = "push"
  role = aws_iam_role.push.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Login"
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Sid      = "Push"
        Effect   = "Allow"
        Action   = local.push_actions
        Resource = [for repository in aws_ecr_repository.image : repository.arn]
      },
    ]
  })
}
