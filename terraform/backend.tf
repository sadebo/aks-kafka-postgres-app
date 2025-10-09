terraform {
  required_version = ">= 1.6.0"
  backend "azurerm" {
    resource_group_name  = "tfstate-rg"
    storage_account_name = "tfstflask1234"   # must be globally unique
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
  }
}
#   required_providers {
#     azurerm = {
#       source  = "hashicorp/azurerm"
#       version = "~> 3.100"
#     }
#     random = {
#       source  = "hashicorp/random"
#       version = "~> 3.5"
#     }
#   }
# }
