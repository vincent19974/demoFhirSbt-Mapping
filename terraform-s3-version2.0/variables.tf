variable "bucket" {
    type = string
    description = "Name of the bucket"
}

variable "acl" {
    type = string
    description = "ACL (PRIVATE OR NOT)"
}

variable "name" {
    type = string
    description = "Name of the tag for s3"
}

variable "environment" {
    type = string
    description = "Env for this bucket"
}

variable "region" {
    type = string
    description = "Region var"
}


