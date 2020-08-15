## Terraform

1. Install terraform and setup aws (https://learn.hashicorp.com/tutorials/terraform/install-cli)
2. Create resorce file `(s3.tf)`
3. Write code for resorce file 
4. Create `variable.tf` file
5. Change every hardcode variable name in `s3.tf` with `var.var_name`, and make variables with description in `variables.tf` file
6. Create `terraform.tfvars`
7. Now in `terraform.tfvars` you can hardocde your variables
8. Create `output.tf` for possible outputs if u need
9. Go in terminal to folder which contains terraform project
10. Run `terraform init` for initialization
11. Run `terraform plan` to check what are you building
12. When evrything is ready run `terraform apply`
13. Now u can go to aws and check if bucket is created or run aws s3 ls command   
14. After apply is done - go to aws and check if bucket is created or run aws s3 ls command
